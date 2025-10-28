// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010.

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>
#include <vector>
#include <unistd.h>
#include <mutex>
#include "applications/application.h"
#include "common/utils.h"
#include "common/connection.h"
#include "common/definitions.hh"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"
#include "common/debug.hh"

using std::map;
using std::pair;
using std::string;
using std::vector;
using std::tr1::unordered_map;

std::atomic<uint64_t> DeterministicScheduler::ro_rr_ticket_{0};

// RODispatcherThread: Handles read-only transactions.
void *DeterministicScheduler::RODispatcherThread(void *arg)
{
    using PairT = std::pair<int, DeterministicScheduler *>;
    PairT *args = reinterpret_cast<PairT *>(arg);
    const int dispatcher_id = args->first;
    DeterministicScheduler *scheduler = args->second;
    delete args;

    PrintCpu("RO Dispatcher", dispatcher_id);

    // 公開済みの最新エポックを取得するために SimpleStorage にキャスト
    // ※ SimpleStorage に CurrentEpoch() のアクセサを用意してください（下に例を記載）
    SimpleStorage* simple_storage = static_cast<SimpleStorage*>(scheduler->storage_);

    MessageProto message;
    while (true)
    {
        // 1) RO用コネクションからバッチ受信
        if (!(*scheduler->ro_connections_)[dispatcher_id]->GetMessage(&message)) {
            usleep(50);
            continue;
        }
        assert(message.type() == MessageProto::TXN_BATCH);

        const double batch_recv_time = GetTime();

        // 2) RO件数カウント
        int ro_cnt = 0;
        for (int i = 0; i < message.data_ptr_size(); ++i) {
            auto raw_ptr = message.data_ptr(i);
            TxnProto *t = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw_ptr));
            if (t->has_read_only() && t->read_only()) ++ro_cnt;
        }
        if (ro_cnt == 0) continue;

        // 3) ワーカーへのRR割り振り
        const uint64_t base = scheduler->ro_rr_ticket_.fetch_add(ro_cnt, std::memory_order_relaxed);
        uint64_t local = 0;

        for (int i = 0; i < message.data_ptr_size(); ++i)
        {
            auto raw_ptr = message.data_ptr(i);
            TxnProto *txn = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw_ptr));
            if (!(txn->has_read_only() && txn->read_only())) continue;

            txn->set_time_sequencer_begin(batch_recv_time);
            txn->set_time_sequencer_end(GetTime());

            // ✅ 公開済みの最新スナップショットを読む
            const int64_t snap_ep = simple_storage->CurrentEpoch();  // ★要アクセサ
            // cut（txn_idの上限値）もメタとしてセット
            const int64_t snap_tx =
                snap_ep * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);

            txn->set_snapshot_epoch(snap_ep);
            txn->set_snapshot_txn_id(snap_tx);

            // RCU: 該当エポックを Pin（公開済みなので必ず存在）
            scheduler->storage_->PinEpoch(snap_ep);

            // ワーカーへディスパッチ
            scheduler->executing_txns_++;
            const uint64_t dest = (base + local) % NUM_WORKERS;
            ++local;
            scheduler->ro_queues_[dest]->Push(txn);
        }
    }
    return nullptr;
}



DeterministicScheduler::DeterministicScheduler(Configuration *conf,
                                               Connection *rw_connection,
                                               std::vector<Connection *> *ro_connections,
                                               Storage *storage,
                                               const Application *application)
    : configuration_(conf),
      rw_connection_(rw_connection),
      ro_connections_(ro_connections),
      storage_(storage),
      application_(application)
{
    ready_txns_ = new std::deque<TxnProto *>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
    rw_txns_queue_ = new AtomicQueue<TxnProto *>();
    done_queue = new AtomicQueue<TxnProto *>();
    
    // ▼ 追加 ▼
    // SimpleStorage::SnapshotRequest* を保持するキューを初期化
    snapshot_queue_ = new AtomicQueue<SimpleStorage::SnapshotRequest*>();
    // ▲ 追加 ▲

    for (int i = 0; i < NUM_WORKERS; ++i) {
        message_queues[i] = new AtomicQueue<MessageProto>();
        ro_queues_[i] = new AtomicQueue<TxnProto *>();
    }

    // ... (レイテンシ計測用の変数の初期化) ...
    total_ro_dispatch_time_ = 0;
    total_ro_queueing_time_ = 0;
    total_ro_worker_time_ = 0;
    processed_rot_count_ = 0;
    total_sequencer_time_ = 0;
    total_queueing_time_ = 0;
    total_worker_time_ = 0;
    processed_rwt_count_ = 0;

    Spin(1);

    cpu_set_t cpuset;

    // RO Dispatcher スレッドの起動
    for (int i = 0; i < NUM_RO_DISPATCHERS; ++i) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_RO_DISPATCHER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        auto arg = new std::pair<int, DeterministicScheduler *>(i, this);
        pthread_create(&(ro_dispatcher_threads_[i]), &attr, RODispatcherThread, reinterpret_cast<void *>(arg));
    }

    // Lock Manager スレッドの起動
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(LOCK_MANAGER_CORE, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&lock_manager_thread_, &attr, LockManagerThread, reinterpret_cast<void *>(this));
    }

    // ▼ 追加 ▼
    // Snapshot スレッドの起動
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        // definitions.hh などで SNAPSHOT_THREAD_CORE を定義してください
        // (例: #define SNAPSHOT_THREAD_CORE (LOCK_MANAGER_CORE + 1))
        CPU_SET(SNAPSHOT_THREAD_CORE, &cpuset); 
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&snapshot_thread_, &attr, SnapshotThreadMain, reinterpret_cast<void *>(this));
    }
    // ▲ 追加 ▲

    // Worker スレッドの起動
    for (int i = 0; i < NUM_WORKERS; ++i) {
        std::string channel("scheduler");
        channel.append(IntToString(i));
        thread_connections_[i] = rw_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_WORKER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&(threads_[i]), &attr, RunWorkerThread, reinterpret_cast<void *>(new std::pair<int, DeterministicScheduler *>(i, this)));
    }
}
DeterministicScheduler::~DeterministicScheduler()
{
    delete ready_txns_;
    delete lock_manager_;
    delete rw_txns_queue_;
    delete done_queue;
    for (int i = 0; i < NUM_WORKERS; i++) {
        delete message_queues[i];
        delete ro_queues_[i];
    }
}

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    using pair_t = std::pair<int, DeterministicScheduler *>;
    pair_t *args_pair = reinterpret_cast<pair_t *>(arg);
    const int thread_id = args_pair->first;
    DeterministicScheduler *scheduler = args_pair->second;
    delete args_pair;

    std::tr1::unordered_map<std::string, StorageManager *> active_txns;
    PrintCpu("Worker", thread_id);

    MessageProto message;
    while (true)
    {
        if (scheduler->message_queues[thread_id]->Pop(&message)) {
            assert(message.type() == MessageProto::READ_RESULT);
            const std::string &chan = message.destination_channel();
            auto it = active_txns.find(chan);
            if (it != active_txns.end()) {
                StorageManager *manager = it->second;
                manager->HandleReadResult(message);
                if (manager->ReadyToExecute()) {
                    TxnProto *txn = manager->txn_;
                    scheduler->application_->Execute(txn, manager);
                    txn->set_time_worker_end(GetTime());
                    delete manager;
                    scheduler->thread_connections_[thread_id]->UnlinkChannel(IntToString(txn->txn_id()));
                    active_txns.erase(chan);
                    scheduler->done_queue->Push(txn);
                }
            }
            continue;
        }

        TxnProto *txn = NULL;
        if (!scheduler->rw_txns_queue_->Pop(&txn)) {
            if (!scheduler->ro_queues_[thread_id]->Pop(&txn)) {
                //int victim_id = rand() % NUM_WORKERS;
                // if (victim_id != thread_id) {
                //     (void)scheduler->ro_queues_[victim_id]->Pop(&txn);
                // }
            }
        }

        if (txn == NULL) {
            usleep(100);
            continue;
        }

        txn->set_time_worker_begin(GetTime());

        if (txn->read_only()) {
            const int64_t snap_txn = txn->snapshot_txn_id();
            for (int i = 0; i < txn->read_set_size(); ++i) {
                (void)scheduler->storage_->ReadObject(txn->read_set(i), snap_txn);
            }
            for (int i = 0; i < txn->read_write_set_size(); ++i) {
                (void)scheduler->storage_->ReadObject(txn->read_write_set(i), snap_txn);
            }
            txn->set_time_worker_end(GetTime());

            // ▼▼▼ 修正点 ▼▼▼
            // Unpin は done_queue に積む *前* に行う
            // （LockManagerThread が snapshot_queue_ でブロックしていても Unpin が実行されるようにするため）
            if (txn->has_snapshot_epoch()) {
                scheduler->storage_->UnpinEpoch(txn->snapshot_epoch());
            }
            // ▲▲▲ 修正点 ▲▲▲

            scheduler->done_queue->Push(txn);
            continue;
        }

        StorageManager *manager = new StorageManager(
            scheduler->configuration_, scheduler->thread_connections_[thread_id],
            scheduler->storage_, txn);

        if (manager->ReadyToExecute()) {
            scheduler->application_->Execute(txn, manager);
            txn->set_time_worker_end(GetTime());
            delete manager;
            scheduler->done_queue->Push(txn);
        } else {
            const std::string chan = IntToString(txn->txn_id());
            scheduler->thread_connections_[thread_id]->LinkChannel(chan);
            active_txns[chan] = manager;
        }
    }
    return NULL;
}

MessageProto *GetBatch(int batch_id, Connection *connection, unordered_map<int, MessageProto *> *batches)
{
    if (batches->count(batch_id) > 0)
    {
        MessageProto *batch = (*batches)[batch_id];
        batches->erase(batch_id);
        return batch;
    }
    else
    {
        MessageProto *message = new MessageProto();
        while (connection->GetMessage(message))
        {
            assert(message->type() == MessageProto::TXN_BATCH);
            if (message->batch_number() == batch_id)
            {
                return message;
            }
            else
            {
                (*batches)[message->batch_number()] = message;
                message = new MessageProto();
            }
        }
        delete message;
        return NULL;
    }
}

void* DeterministicScheduler::SnapshotThreadMain(void* arg) {
    DeterministicScheduler* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);
    PrintCpu("Snapshotter", 0);  // CPUアフィニティ確認

    // Storage* を SimpleStorage* にキャスト（専用APIを使うため）
    SimpleStorage* storage = static_cast<SimpleStorage*>(scheduler->storage_);

    while (true) {
        SimpleStorage::SnapshotRequest* req = nullptr;

        // スナップショット依頼を待つ
        if (!scheduler->snapshot_queue_->Pop(&req)) {
            // ブロッキングキューでない場合の軽いバックオフ
            usleep(500);  // 500us
            continue;
        }

        // publish 対象の new_cut から、RO が読むべき直前エポック (B-1) を算出
        const int64_t new_cut = req->new_cut;
        const int64_t batch_B = static_cast<int64_t>(new_cut / MAX_LOCK_BATCH_SIZE);
        const int64_t wait_ep = std::max<int64_t>(0, batch_B - 1);

        // ★ 要件：RW も RO も終わってから publish する
        // RO の読者数が 0 になるまで待機（Pin/Unpin により進行）
        storage->WaitUntilNoReaders(wait_ep);

        // 低速処理：スナップショットの適用と公開（RCUマップ更新・GC含む）
        storage->ApplyAndPublishSnapshot(req);
        // req は ApplyAndPublishSnapshot 内で delete 済み
    }

    return nullptr;
}


void *DeterministicScheduler::LockManagerThread(void *arg)
{
    PrintCpu("Lock Manager", 0);
    DeterministicScheduler *scheduler = reinterpret_cast<DeterministicScheduler *>(arg);

    // Storage* を SimpleStorage* にキャストしておく
    SimpleStorage* simple_storage = static_cast<SimpleStorage*>(scheduler->storage_);

    auto BatchOfTxn = [](int64_t txn_id) -> int {
        return static_cast<int>(txn_id / MAX_LOCK_BATCH_SIZE);
    };

    // ▼ 修正 ▼
    // [プロデューサ] スナップショット作成 "依頼" を発行するラムダ式
    auto PublishCutForBatch = [&](int B) {
        const int64_t cut = static_cast<int64_t>(B) * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);
        
        // [変更前] 同期的に重い処理を呼び出していた
        // scheduler->storage_->PublishSnapshot(cut); 
        
        // [変更後] 非同期化
        // 1. [高速] デルタを奪取し、リクエストを生成
        SimpleStorage::SnapshotRequest* req = simple_storage->CaptureDeltasAndCreateRequest(cut);
        // 2. [高速] スナップショット作成キューにリクエストを投入
        scheduler->snapshot_queue_->Push(req);
    };
    // ▲ 修正 ▲

    auto AdvanceCommittedPrefixLocked = [&]() {
        for (;;) {
            auto it = scheduler->pending_rw_per_batch_.find(scheduler->next_batch_to_commit_);
            int pending = (it == scheduler->pending_rw_per_batch_.end()) ? 0 : it->second;
            if (pending == 0) {
                if (it != scheduler->pending_rw_per_batch_.end()) {
                    scheduler->pending_rw_per_batch_.erase(it);
                }
                
                // 修正済みの PublishCutForBatch (非同期版) が呼ばれる
                PublishCutForBatch(scheduler->next_batch_to_commit_); 
                
                scheduler->last_committed_batch_.store(scheduler->next_batch_to_commit_, std::memory_order_release);
                scheduler->next_batch_to_commit_++;
            } else {
                break;
            }
        }
    };

    unordered_map<int, MessageProto *> batches;
    MessageProto *batch_message = NULL;
    int txns = 0;
    double time = GetTime();
    int pending_txns = 0;
    int batch_number = 0;
    int empty_poll_streak = 0;
    const int kBackoffFloorUs = 200;
    const int kBackoffCeilUs = 5000;

    while (true)
    {
        TxnProto *done_txn;
        // 1. 完了キュー (done_queue) の処理
        if (scheduler->done_queue->Pop(&done_txn)) {
            empty_poll_streak = 0;
            if (done_txn->read_only()) {
                // ... (RO完了処理、統計更新) ...
                
                // ▼▼▼ 修正点 ▼▼▼
                // UnpinEpoch は WorkerThread 側で実行済みのため、ここでは削除
                // if (done_txn->has_snapshot_epoch()) {
                //     scheduler->storage_->UnpinEpoch(done_txn->snapshot_epoch());
                // }
                // ▲▲▲ 修正点 ▲▲▲

                if (done_txn->has_time_sequencer_begin()) {
                    double d = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double current = scheduler->total_ro_dispatch_time_.load();
                    while (!scheduler->total_ro_dispatch_time_.compare_exchange_weak(current, current + d)) {}
                    current = scheduler->total_ro_queueing_time_.load();
                    while (!scheduler->total_ro_queueing_time_.compare_exchange_weak(current, current + q)) {}
                    current = scheduler->total_ro_worker_time_.load();
                    while (!scheduler->total_ro_worker_time_.compare_exchange_weak(current, current + w)) {}
                    scheduler->processed_rot_count_++;
                }
            } else {
                // RW完了処理
                scheduler->lock_manager_->Release(done_txn);
                {
                    // バッチの残りトランザクション数をデクリメント
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    int B = BatchOfTxn(done_txn->txn_id());
                    auto it = scheduler->pending_rw_per_batch_.find(B);
                    if (it != scheduler->pending_rw_per_batch_.end()) {
                        it->second--;
                    }
                    // 0になったらスナップショット公開 (非同期依頼)
                    AdvanceCommittedPrefixLocked(); 
                }
                // ... (RW統計更新) ...
                if (done_txn->has_time_sequencer_begin()) {
                    double seq = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double current = scheduler->total_sequencer_time_.load();
                    while (!scheduler->total_sequencer_time_.compare_exchange_weak(current, current + seq)) {}
                    current = scheduler->total_queueing_time_.load();
                    while (!scheduler->total_queueing_time_.compare_exchange_weak(current, current + q)) {}
                    current = scheduler->total_worker_time_.load();
                    while (!scheduler->total_worker_time_.compare_exchange_weak(current, current + w)) {}
                    scheduler->processed_rwt_count_++;
                }
            }
            scheduler->executing_txns_--;
            txns++;
            delete done_txn;
        
        // 2. 新規バッチの受付
        } else {
            if (batch_message == NULL) {
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
            }

            if (batch_message != NULL && pending_txns < MAX_ACTIVE_TXNS) {
                empty_poll_streak = 0;
                const int B = batch_message->batch_number();
                int enqueued = 0;
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    (void)scheduler->pending_rw_per_batch_[B];
                }
                
                for (int i = 0; i < batch_message->data_ptr_size(); i++) {
                    auto raw_ptr = batch_message->data_ptr(i);
                    TxnProto* txn = reinterpret_cast<TxnProto*>(static_cast<uintptr_t>(raw_ptr));

                    txn->set_time_sequencer_begin(GetTime());
                    assert(!txn->read_only());

                    scheduler->lock_manager_->Lock(txn);
                    pending_txns++;
                    enqueued++;
                }

                if (enqueued > 0) {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    scheduler->pending_rw_per_batch_[B] += enqueued;
                }
                
                delete batch_message;
                batch_message = NULL;
                batch_number++;

            } else {
                // 3. アイドル時のバックオフ
                empty_poll_streak = std::min(empty_poll_streak + 1, 1000000);
                int sleep_us = std::min(kBackoffCeilUs, kBackoffFloorUs << std::min(empty_poll_streak, 8));
                usleep(sleep_us);
            }
        }

        // 4. ロック取得済みTXN (ready_txns_) をワーカーに投入
        while (!scheduler->ready_txns_->empty()) {
            TxnProto *txn = scheduler->ready_txns_->front();
            scheduler->ready_txns_->pop_front();
            txn->set_time_sequencer_end(GetTime());
            pending_txns--;
            scheduler->executing_txns_++;
            scheduler->rw_txns_queue_->Push(txn);
        }

        // 5. 統計情報の定期出力
        if (GetTime() > time + 1) {
            double total_time = GetTime() - time;
            double tps = (total_time > 0.0) ? (static_cast<double>(txns) / total_time) : 0.0;
            
            // 人間向け
            std::cout << "Completed " << tps
                      << " txns/sec, " << scheduler->executing_txns_.load()
                      << " executing, " << pending_txns << " pending\n";
            // スクリプト向け
            std::cout << "THROUGHPUT " << tps << "\n"
                      << std::flush;
            
            int processed_rwt = scheduler->processed_rwt_count_.load();
            if (processed_rwt > 0)
            {
                double avg_seq = scheduler->total_sequencer_time_.load() / processed_rwt;
                double avg_q = scheduler->total_queueing_time_.load() / processed_rwt;
                double avg_w = scheduler->total_worker_time_.load() / processed_rwt;
                std::cout << "-------------------- RW Latency (ms) -------------------\n"
                          << "  Sequencer: " << avg_seq * 1000
                          << " | Queueing: " << avg_q * 1000
                          << " | Worker: " << avg_w * 1000 << "\n"
                          << std::flush;
            }
            int processed_rot = scheduler->processed_rot_count_.load();
            if (processed_rot > 0)
            {
                double avg_d = scheduler->total_ro_dispatch_time_.load() / processed_rot;
                double avg_q = scheduler->total_ro_queueing_time_.load() / processed_rot;
                double avg_w = scheduler->total_ro_worker_time_.load() / processed_rot;
                std::cout << "-------------------- RO Latency (ms) -------------------\n"
                          << "  Dispatcher: " << avg_d * 1000
                          << " | Queueing: " << avg_q * 1000
                          << " | Worker: " << avg_w * 1000 << "\n"
                          << std::flush;
            }
            if (processed_rwt > 0 || processed_rot > 0)
            {
                std::cout << "--------------------------------------------------------\n"
                          << std::flush;
            }
            
            time = GetTime();
            txns = 0;
            scheduler->processed_rwt_count_ = 0;
            scheduler->total_sequencer_time_ = 0;
            scheduler->total_queueing_time_ = 0;
            scheduler->total_worker_time_ = 0;
            scheduler->processed_rot_count_ = 0;
            scheduler->total_ro_dispatch_time_ = 0;
            scheduler->total_ro_queueing_time_ = 0;
            scheduler->total_ro_worker_time_ = 0;
        }
    }
}