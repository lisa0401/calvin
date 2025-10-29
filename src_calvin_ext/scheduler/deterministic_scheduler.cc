// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// Deterministic scheduler w/ RO fast-path (no pin), B-1 quiescence publish.

#include "scheduler/deterministic_scheduler.h"

#include <cassert>
#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <vector>
#include <sched.h>
#include <unistd.h>

#include "applications/application.h"
#include "backend/storage_manager.h"
#include "backend/storage.h"
#include "backend/simple_storage.h"
#include "common/connection.h"
#include "common/debug.hh"
#include "common/definitions.hh"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "scheduler/deterministic_lock_manager.h"

using std::string;

std::atomic<uint64_t> DeterministicScheduler::ro_rr_ticket_{0};

// --------- 内部ユーティリティ ---------
static inline int BatchOfTxn(int64_t txn_id) {
    return static_cast<int>(txn_id / MAX_LOCK_BATCH_SIZE);
}

// Sequencer 側のバッチ受信ヘルパ（古いCalvinのまま）
static MessageProto* GetBatch(int batch_id,
                              Connection* connection,
                              std::unordered_map<int, MessageProto *>* batches) {
    auto it = batches->find(batch_id);
    if (it != batches->end()) {
        MessageProto* batch = it->second;
        batches->erase(it);
        return batch;
    } else {
        MessageProto* message = new MessageProto();
        while (connection->GetMessage(message)) {
            assert(message->type() == MessageProto::TXN_BATCH);
            if (message->batch_number() == batch_id) {
                return message;
            } else {
                (*batches)[message->batch_number()] = message;
                message = new MessageProto();
            }
        }
        delete message;
        return nullptr;
    }
}

// --------- RODispatcherThread ---------
// ROのみを受け取り、スナップショット選択/PinせずにRRでワーカーへ投入。
// 併せて B 到着ROは B-1 を読む前提なので、B-1 の quiescence カウンタを加算。
void* DeterministicScheduler::RODispatcherThread(void* arg) {
    using PairT = std::pair<int, DeterministicScheduler *>;
    PairT* args = reinterpret_cast<PairT*>(arg);
    const int dispatcher_id = args->first;
    DeterministicScheduler* scheduler = args->second;
    delete args;

    PrintCpu("RO Dispatcher", dispatcher_id);

    MessageProto message;
    while (true) {
        if (!(*scheduler->ro_connections_)[dispatcher_id]->GetMessage(&message)) {
            usleep(50);
            continue;
        }
        assert(message.type() == MessageProto::TXN_BATCH);
        const double t_recv = GetTime();

        for (int i = 0; i < message.data_ptr_size(); ++i) {
            auto raw_ptr = message.data_ptr(i);
            TxnProto* txn = reinterpret_cast<TxnProto*>(static_cast<uintptr_t>(raw_ptr));
            if (!(txn->has_read_only() && txn->read_only())) continue;

            txn->set_time_sequencer_begin(t_recv);
            txn->set_time_sequencer_end(GetTime());

            const int B = BatchOfTxn(txn->txn_id());
            const int waitB = std::max(0, B - 1);
            {
                std::lock_guard<std::mutex> lk(scheduler->quiescence_mu_);
                scheduler->ro_inflight_per_batch_[waitB] += 1;
            }

            const uint64_t ticket = ro_rr_ticket_.fetch_add(1, std::memory_order_relaxed);
            const uint64_t dest = ticket % NUM_WORKERS;

            scheduler->executing_txns_++;
            scheduler->ro_queues_[dest]->Push(txn);
        }
    }
    return nullptr;
}

// --------- WaitUntilBatchQuiescent ---------
// バッチBの静穏化: 「BのRW==0 && BのRO(inflight)==0」まで待つ。
// ここで待つBはPublish対象の (B) で、ROは (B+1) 到着時に B を読み終える契機になる想定。
void DeterministicScheduler::WaitUntilBatchQuiescent(int B) {
    std::unique_lock<std::mutex> lk(quiescence_mu_);
    quiescence_cv_.wait(lk, [&]{
        int rw = 0, ro = 0;
        {
            std::lock_guard<std::mutex> lk2(pending_mu_);
            auto it = pending_rw_per_batch_.find(B);
            rw = (it == pending_rw_per_batch_.end()) ? 0 : it->second;
        }
        auto it2 = ro_inflight_per_batch_.find(B);
        ro = (it2 == ro_inflight_per_batch_.end()) ? 0 : it2->second;
        return (rw == 0 && ro == 0);
    });
    ro_inflight_per_batch_.erase(B);
}

// --------- コンストラクタ / デストラクタ ---------
DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* rw_connection,
                                               std::vector<Connection*>* ro_connections,
                                               Storage* storage,
                                               const Application* application)
    : configuration_(conf),
      rw_connection_(rw_connection),
      ro_connections_(ro_connections),
      storage_(storage),
      application_(application) {
    ready_txns_   = new std::deque<TxnProto*>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);
    rw_txns_queue_= new AtomicQueue<TxnProto*>();
    done_queue    = new AtomicQueue<TxnProto*>();
    snapshot_queue_ = new AtomicQueue<SimpleStorage::SnapshotRequest*>();

    for (int i = 0; i < NUM_WORKERS; ++i) {
        message_queues[i] = new AtomicQueue<MessageProto>();
        ro_queues_[i]     = new AtomicQueue<TxnProto*>();
    }

    total_ro_dispatch_time_ = 0;
    total_ro_queueing_time_ = 0;
    total_ro_worker_time_   = 0;
    processed_rot_count_    = 0;
    total_sequencer_time_   = 0;
    total_queueing_time_    = 0;
    total_worker_time_      = 0;
    processed_rwt_count_    = 0;

    Spin(1);

    cpu_set_t cpuset;

    // RO Dispatcher(s)
    for (int i = 0; i < NUM_RO_DISPATCHERS; ++i) {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_RO_DISPATCHER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        auto arg = new std::pair<int, DeterministicScheduler*>(i, this);
        pthread_create(&(ro_dispatcher_threads_[i]), &attr, RODispatcherThread,
                       reinterpret_cast<void*>(arg));
    }

    // Lock Manager
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(LOCK_MANAGER_CORE, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&lock_manager_thread_, &attr, LockManagerThread,
                       reinterpret_cast<void*>(this));
    }

    // Snapshot thread
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(SNAPSHOT_THREAD_CORE, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&snapshot_thread_, &attr, SnapshotThreadMain,
                       reinterpret_cast<void*>(this));
    }

    // Workers
    for (int i = 0; i < NUM_WORKERS; ++i) {
        std::string channel("scheduler");
        channel.append(IntToString(i));
        thread_connections_[i] =
            rw_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_WORKER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&(threads_[i]), &attr, RunWorkerThread,
                       reinterpret_cast<void*>(new std::pair<int, DeterministicScheduler*>(i, this)));
    }
}

DeterministicScheduler::~DeterministicScheduler() {
    delete ready_txns_;
    delete lock_manager_;
    delete rw_txns_queue_;
    delete done_queue;
    delete snapshot_queue_;
    for (int i = 0; i < NUM_WORKERS; ++i) {
        delete message_queues[i];
        delete ro_queues_[i];
    }
}
namespace {
// TxnProto に read_only フィールドが埋まっていればそれを使い、
// 念のため write_set/read_write_set が空かでも判定できるようにする二段構え。
inline bool IsROTxn(const TxnProto* t) {
    // read_only フィールドを Sequencer で必ず設定している前提
    if (t->has_read_only()) return t->read_only();
    // バックストップ（安全側）：書き込みが1つも無ければRO扱い
    return (t->write_set_size() == 0 && t->read_write_set_size() == 0);
}

// [★ 修正] ApplyWritesToStorage 関数は StorageManager を使う設計では不要なため削除

} // namespace

// --------- Worker (improved) ---------
// RO: 最新公開cut or txn->snapshot_txn_id を cut→epoch 変換して ReadObjectAtEpoch。
// RW: StorageManager 経由で従来どおり。
void* DeterministicScheduler::RunWorkerThread(void* arg) {
    using pair_t = std::pair<int, DeterministicScheduler *>;
    pair_t* args_pair = reinterpret_cast<pair_t*>(arg);
    const int thread_id = args_pair->first;
    DeterministicScheduler* scheduler = args_pair->second;
    delete args_pair;

    std::tr1::unordered_map<std::string, StorageManager*> active_txns;
    PrintCpu("Worker", thread_id);

    MessageProto message;
    while (true) {
        // 1) 非同期 READ_RESULT の回収（従来どおり）
        if (scheduler->message_queues[thread_id]->Pop(&message)) {
            assert(message.type() == MessageProto::READ_RESULT);
            const std::string& chan = message.destination_channel();
            auto it = active_txns.find(chan);
            if (it != active_txns.end()) {
                StorageManager* manager = it->second;
                manager->HandleReadResult(message);
                if (manager->ReadyToExecute()) {
                    TxnProto* txn = manager->txn_;
                    scheduler->application_->Execute(txn, manager); // 1. 実行

                    // [★ 修正] ApplyWritesToStorage 呼び出しを削除

                    txn->set_time_worker_end(GetTime());
                    delete manager;
                    scheduler->thread_connections_[thread_id]->UnlinkChannel(IntToString(txn->txn_id()));
                    active_txns.erase(chan);
                    scheduler->done_queue->Push(txn); // 2. 完了
                }
            }
            continue;
        }

        // 2) キューから新規 Txn を取得（RW→RO の優先は任意。ここでは RW を先に試す）
        TxnProto* txn = nullptr;
        if (!scheduler->rw_txns_queue_->Pop(&txn)) {
            (void)scheduler->ro_queues_[thread_id]->Pop(&txn);
        }
        if (!txn) { usleep(100); continue; }

        txn->set_time_worker_begin(GetTime());

        // 3) RO fast-path
        if (IsROTxn(txn)) {
            auto* ss = static_cast<SimpleStorage*>(scheduler->storage_);

            // cut の決定：Txn に snapshot_cut があればそれを、無ければ最新公開 cut
            int64_t cut = 0;
            cut = ss->LatestPublishedCut();

            // cut → epoch 変換してスナップショット読み
            const int64_t epoch = ss->EpochForCut(cut);

            // [★ 修正] Read起因UAF(Bug 2)対策：エポックをピン留め
            ss->PinEpoch(epoch);

            for (int i = 0; i < txn->read_set_size(); ++i) {
                (void)ss->ReadObjectAtEpoch(txn->read_set(i), epoch);
            }
            for (int i = 0; i < txn->read_write_set_size(); ++i) {
                // [★ 修正] .read_write_set_size(i) -> .read_write_set(i)
                (void)ss->ReadObjectAtEpoch(txn->read_write_set(i), epoch);
            }

            // [★ 修正] ピン留めを解除
            ss->UnpinEpoch(epoch);

            txn->set_time_worker_end(GetTime());
            scheduler->done_queue->Push(txn);
            continue;
        }

        // 4) RW 通常パス（従来どおり）
        StorageManager* manager = new StorageManager(
            scheduler->configuration_, scheduler->thread_connections_[thread_id],
            scheduler->storage_, txn);

        if (manager->ReadyToExecute()) {
            scheduler->application_->Execute(txn, manager); // 1. 実行

            // [★ 修正] ApplyWritesToStorage 呼び出しを削除
            
            txn->set_time_worker_end(GetTime());
            delete manager;
            scheduler->done_queue->Push(txn); // 2. 完了
        } else {
            const std::string chan = IntToString(txn->txn_id());
            scheduler->thread_connections_[thread_id]->LinkChannel(chan);
            active_txns[chan] = manager;
        }
    }
    return nullptr;
}

// --------- SnapshotThreadMain ---------
// LockManagerThread から入る publish 依頼を受け、対象 B-1 の quiescence を待って公開。
void* DeterministicScheduler::SnapshotThreadMain(void* arg) {
    auto* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);
    PrintCpu("Snapshotter", 0);

    auto* storage = static_cast<SimpleStorage*>(scheduler->storage_);

    while (true) {
        SimpleStorage::SnapshotRequest* req = nullptr;
        if (!scheduler->snapshot_queue_->Pop(&req)) {
            usleep(500);
            continue;
        }

        const int64_t new_cut = req->new_cut;
        const int64_t batch_B = static_cast<int64_t>(new_cut / MAX_LOCK_BATCH_SIZE);
        const int waitB = static_cast<int>(std::max<int64_t>(0, batch_B - 1));

        // 対象バッチ(waitB)の RW と RO が両方完了してから publish
        scheduler->WaitUntilBatchQuiescent(waitB);

        storage->ApplyAndPublishSnapshot(req); // reqは内部でdelete
    }

    return nullptr;
}

// --------- LockManagerThread ---------
// done_queue の回収、RWロック管理、RW残件数の更新、連続バッチの publish 依頼生成、
// そして RO/RW 完了時の quiescence 通知を担当。
void* DeterministicScheduler::LockManagerThread(void* arg) {
    auto* scheduler = reinterpret_cast<DeterministicScheduler*>(arg);
    PrintCpu("Lock Manager", 0);

    auto* simple_storage = static_cast<SimpleStorage*>(scheduler->storage_);

    auto PublishCutForBatch = [&](int B) {
        const int64_t cut = static_cast<int64_t>(B) * MAX_LOCK_BATCH_SIZE
                            + (MAX_LOCK_BATCH_SIZE - 1);
        SimpleStorage::SnapshotRequest* req =
            simple_storage->CaptureDeltasAndCreateRequest(cut);
        scheduler->snapshot_queue_->Push(req);
    };

    auto AdvanceCommittedPrefixLocked = [&]() {
        for (;;) {
            auto it = scheduler->pending_rw_per_batch_.find(scheduler->next_batch_to_commit_);
            int pending = (it == scheduler->pending_rw_per_batch_.end()) ? 0 : it->second;
            if (pending == 0) {
                if (it != scheduler->pending_rw_per_batch_.end()) {
                    scheduler->pending_rw_per_batch_.erase(it);
                }
                PublishCutForBatch(scheduler->next_batch_to_commit_);
                scheduler->last_committed_batch_.store(
                    scheduler->next_batch_to_commit_, std::memory_order_release);
                scheduler->next_batch_to_commit_++;
            } else {
                break;
            }
        }
    };

    std::unordered_map<int, MessageProto*> batches;
    MessageProto* batch_message = nullptr;
    int txns = 0;
    double time0 = GetTime();
    int pending_txns = 0;
    int batch_number = 0;
    int empty_poll_streak = 0;
    const int kBackoffFloorUs = 200;
    const int kBackoffCeilUs  = 5000;

    while (true) {
        TxnProto* done_txn = nullptr;

        // 1) 完了キュー処理
        if (scheduler->done_queue->Pop(&done_txn)) {
            empty_poll_streak = 0;

            if (done_txn->read_only()) {
                // RO 完了 → B-1 のカウンタをデクリメントして通知
                const int B = BatchOfTxn(done_txn->txn_id());
                const int waitB = std::max(0, B - 1);
                {
                    std::lock_guard<std::mutex> lk(scheduler->quiescence_mu_);
                    auto& cnt = scheduler->ro_inflight_per_batch_[waitB];
                    if (cnt > 0) --cnt;
                    scheduler->quiescence_cv_.notify_all();
                }

                // 計測
                if (done_txn->has_time_sequencer_begin()) {
                    double d = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double cur = scheduler->total_ro_dispatch_time_.load();
                    while (!scheduler->total_ro_dispatch_time_.compare_exchange_weak(cur, cur + d)) {}
                    cur = scheduler->total_ro_queueing_time_.load();
                    while (!scheduler->total_ro_queueing_time_.compare_exchange_weak(cur, cur + q)) {}
                    cur = scheduler->total_ro_worker_time_.load();
                    while (!scheduler->total_ro_worker_time_.compare_exchange_weak(cur, cur + w)) {}
                    scheduler->processed_rot_count_++;
                }
            } else {
                // RW 完了 → ロック解放＋残数更新＋通知
                scheduler->lock_manager_->Release(done_txn);

                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    const int B = BatchOfTxn(done_txn->txn_id());
                    auto it = scheduler->pending_rw_per_batch_.find(B);
                    if (it != scheduler->pending_rw_per_batch_.end()) {
                        it->second--;
                    }
                }
                {
                    std::lock_guard<std::mutex> lk2(scheduler->quiescence_mu_);
                    scheduler->quiescence_cv_.notify_all();
                }
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    AdvanceCommittedPrefixLocked();
                }

                // 計測
                if (done_txn->has_time_sequencer_begin()) {
                    double seq = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q   = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w   = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double cur = scheduler->total_sequencer_time_.load();
                    while (!scheduler->total_sequencer_time_.compare_exchange_weak(cur, cur + seq)) {}
                    cur = scheduler->total_queueing_time_.load();
                    while (!scheduler->total_queueing_time_.compare_exchange_weak(cur, cur + q)) {}
                    cur = scheduler->total_worker_time_.load();
                    while (!scheduler->total_worker_time_.compare_exchange_weak(cur, cur + w)) {}
                    scheduler->processed_rwt_count_++;
                }
            }

            scheduler->executing_txns_--;
            txns++;
            delete done_txn;

        // 2) 新規バッチ受付（RW用チャネル）
        } else {
            if (batch_message == nullptr) {
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
            }

            if (batch_message != nullptr && pending_txns < MAX_ACTIVE_TXNS) {
                empty_poll_streak = 0;
                const int B = batch_message->batch_number();
                int enqueued = 0;

                // 0初期化確保
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    (void)scheduler->pending_rw_per_batch_[B];
                }

                for (int i = 0; i < batch_message->data_ptr_size(); i++) {
                    auto raw_ptr = batch_message->data_ptr(i);
                    TxnProto* txn = reinterpret_cast<TxnProto*>(static_cast<uintptr_t>(raw_ptr));
                    txn->set_time_sequencer_begin(GetTime());
                    assert(!txn->read_only()); // RWバッチ想定

                    scheduler->lock_manager_->Lock(txn);
                    pending_txns++;
                    enqueued++;
                }

                if (enqueued > 0) {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    scheduler->pending_rw_per_batch_[B] += enqueued;
                }

                delete batch_message;
                batch_message = nullptr;
                batch_number++;

            } else {
                empty_poll_streak = std::min(empty_poll_streak + 1, 1000000);
                int sleep_us = std::min(kBackoffCeilUs,
                                        kBackoffFloorUs << std::min(empty_poll_streak, 8));
                usleep(sleep_us);
            }
        }

        // 3) ロック取得済みRWをワーカーへ
        while (!scheduler->ready_txns_->empty()) {
            TxnProto* txn = scheduler->ready_txns_->front();
            scheduler->ready_txns_->pop_front();
            txn->set_time_sequencer_end(GetTime());
            pending_txns--;
            scheduler->executing_txns_++;
            scheduler->rw_txns_queue_->Push(txn);
        }

        // 4) 1秒ごとの統計出力
        if (GetTime() > time0 + 1) {
            double total_time = GetTime() - time0;
            double tps = (total_time > 0.0) ? (static_cast<double>(txns) / total_time) : 0.0;

            std::cout << "Completed " << tps
                      << " txns/sec, " << scheduler->executing_txns_.load()
                      << " executing, " << pending_txns << " pending\n";
            std::cout << "THROUGHPUT " << tps << "\n" << std::flush;

            int processed_rwt = scheduler->processed_rwt_count_.load();
            if (processed_rwt > 0) {
                double avg_seq = scheduler->total_sequencer_time_.load() / processed_rwt;
                double avg_q   = scheduler->total_queueing_time_.load() / processed_rwt;
                double avg_w   = scheduler->total_worker_time_.load() / processed_rwt;
                std::cout << "-------------------- RW Latency (ms) -------------------\n"
                          << "  Sequencer: " << avg_seq * 1000
                          << " | Queueing: " << avg_q * 1000
                          << " | Worker: " << avg_w * 1000 << "\n";
            }
            int processed_rot = scheduler->processed_rot_count_.load();
            if (processed_rot > 0) {
                double avg_d = scheduler->total_ro_dispatch_time_.load() / processed_rot;
                double avg_q = scheduler->total_ro_queueing_time_.load() / processed_rot;
                double avg_w = scheduler->total_ro_worker_time_.load() / processed_rot;
                std::cout << "-------------------- RO Latency (ms) -------------------\n"
                          << "  Dispatcher: " << avg_d * 1000
                          << " | Queueing: " << avg_q * 1000
                          << " | Worker: " << avg_w * 1000 << "\n";
            }
            if (processed_rwt > 0 || processed_rot > 0) {
                std::cout << "--------------------------------------------------------\n"
                          << std::flush;
            }

            time0 = GetTime();
            txns = 0;
            scheduler->processed_rwt_count_    = 0;
            scheduler->total_sequencer_time_   = 0;
            scheduler->total_queueing_time_    = 0;
            scheduler->total_worker_time_      = 0;
            scheduler->processed_rot_count_    = 0;
            scheduler->total_ro_dispatch_time_ = 0;
            scheduler->total_ro_queueing_time_ = 0;
            scheduler->total_ro_worker_time_   = 0;
        }
    }
    return nullptr;
}

// --------- (Optional) ZMQ utilities (未使用なら空実装で可) ---------
void DeterministicScheduler::SendTxnPtr(zmq::socket_t* /*socket*/, TxnProto* /*txn*/) {}
TxnProto* DeterministicScheduler::GetTxnPtr(zmq::socket_t* /*socket*/, zmq::message_t* /*msg*/) {
    return nullptr;
}