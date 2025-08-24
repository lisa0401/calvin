#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>
#include <vector>
#include <unistd.h> // usleep()のために追加
#include <mutex>
#include "applications/application.h"
#include "common/utils.h"
#include "common/zmq.hpp"
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
using zmq::socket_t;
std::atomic<uint64_t> DeterministicScheduler::ro_rr_ticket_{0};
static void DeleteTxnPtr(void *data, void *hint)
{
    free(data);
}

// RODispatcherThread: グローバル・ラウンドロビン版（高スキューでも均等配送）
void *DeterministicScheduler::RODispatcherThread(void *arg)
{
    using PairT = std::pair<int, DeterministicScheduler *>;
    PairT *args = reinterpret_cast<PairT *>(arg);
    const int dispatcher_id = args->first;
    DeterministicScheduler *scheduler = args->second;
    delete args;

    PrintCpu("RO Dispatcher", dispatcher_id);

    MessageProto message;
    while (true)
    {
        if (!(*scheduler->ro_connections_)[dispatcher_id]->GetMessage(&message))
        {
            usleep(50);
            continue;
        }
        assert(message.type() == MessageProto::TXN_BATCH);

        const double batch_recv_time = GetTime();

        // RO は「最新コミットの1つ前エポック」を読む
        const uint64_t latest = scheduler->last_committed_batch_.load(std::memory_order_acquire);
        const uint64_t snap_ep = (latest > 0) ? (latest - 1) : 0;
        const int64_t snap_tx =
            static_cast<int64_t>(snap_ep) * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);

        const int n = message.data_ptr_size();

        // RRチケットを “RO 件数” ぶんだけ消費したいので、まず RO 件数を数える
        int ro_cnt = 0;
        for (int i = 0; i < n; ++i)
        {
            auto raw_ptr = message.data_ptr(i);
            TxnProto *t = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw_ptr));
            if (t->has_read_only() && t->read_only())
                ++ro_cnt;
        }
        if (ro_cnt == 0)
            continue;

        // このバッチの RO 件数ぶんのチケットをまとめ取り
        const uint64_t base = ro_rr_ticket_.fetch_add(ro_cnt, std::memory_order_relaxed);
        uint64_t local = 0;

        for (int i = 0; i < n; ++i)
        {
            auto raw_ptr = message.data_ptr(i);
            TxnProto *txn = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw_ptr));

            if (!(txn->has_read_only() && txn->read_only()))
            {
                // RW が紛れていても ASSERT しないで無視（RW は別経路で処理）
                continue;
            }

            // 計測（Dispatcher 区間）
            txn->set_time_sequencer_begin(batch_recv_time);
            txn->set_time_sequencer_end(GetTime());

            // スナップショット境界（前エポック）を付与
            txn->set_snapshot_epoch(snap_ep);
            txn->set_snapshot_txn_id(snap_tx);

            scheduler->executing_txns_++;

            // グローバル・ラウンドロビンで均等配送（スキュー無視でOK）
            const uint64_t dest = (base + local) % NUM_WORKERS;
            ++local;

            scheduler->ro_queues_[dest]->Push(txn);
        }
    }
    return nullptr;
}

void DeterministicScheduler::SendTxnPtr(socket_t *socket, TxnProto *txn)
{
    TxnProto **txn_ptr = reinterpret_cast<TxnProto **>(malloc(sizeof(txn)));
    *txn_ptr = txn;
    zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
    socket->send(msg);
}

TxnProto *DeterministicScheduler::GetTxnPtr(socket_t *socket, zmq::message_t *msg)
{
    if (!socket->recv(msg, ZMQ_NOBLOCK))
        return NULL;
    TxnProto *txn = *reinterpret_cast<TxnProto **>(msg->data());
    return txn;
}

// ★★★ 修正箇所 No.2 ★★★
// コンストラクタ: 複数のRO Connectionを受け取り、複数のDispatcherスレッドを生成
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

    for (int i = 0; i < NUM_WORKERS; ++i)
    {
        message_queues[i] = new AtomicQueue<MessageProto>();
    }
    // ★ ROロックフリーキューの生成
    for (int i = 0; i < NUM_WORKERS; ++i)
    {
        ro_queues_[i] = new AtomicQueue<TxnProto *>();
    }

    // 計測の初期値（atomic はデフォルト0だが明示）
    total_ro_dispatch_time_ = 0;
    total_ro_queueing_time_ = 0;
    total_ro_worker_time_ = 0;
    processed_rot_count_ = 0;
    total_sequencer_time_ = 0;
    total_queueing_time_ = 0;
    total_worker_time_ = 0;
    processed_rwt_count_ = 0;

    Spin(1); // 既存の小休止

    cpu_set_t cpuset;

    // === RO Dispatcher スレッド ===
    for (int i = 0; i < NUM_RO_DISPATCHERS; ++i)
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_RO_DISPATCHER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

        std::pair<int, DeterministicScheduler *> *arg =
            new std::pair<int, DeterministicScheduler *>(i, this);
        pthread_create(&(ro_dispatcher_threads_[i]), &attr,
                       RODispatcherThread, reinterpret_cast<void *>(arg));
    }

    // === Lock Manager スレッド ===
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(LOCK_MANAGER_CORE, &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&lock_manager_thread_, &attr,
                       LockManagerThread, reinterpret_cast<void *>(this));
    }

    // === Worker スレッド ===
    for (int i = 0; i < NUM_WORKERS; ++i)
    {
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
                       reinterpret_cast<void *>(new std::pair<int, DeterministicScheduler *>(i, this)));
    }
}

DeterministicScheduler::~DeterministicScheduler()
{
    delete ready_txns_;
    delete lock_manager_;
    delete rw_txns_queue_;
    delete done_queue;
    for (int i = 0; i < NUM_WORKERS; i++)
    {
        delete message_queues[i];
    }
    // ★ 追加: ROキューの破棄
    for (int i = 0; i < NUM_WORKERS; i++)
    {
        delete ro_queues_[i];
    }
}

// RunWorkerThread (最終修正版)

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    using pair_t = std::pair<int, DeterministicScheduler *>;
    pair_t *args_pair = reinterpret_cast<pair_t *>(arg);
    const int thread_id = args_pair->first;
    DeterministicScheduler *scheduler = args_pair->second;
    delete args_pair;

    // RW/RO 共通ワーカー。最初に RW キューを確認し、その後 RO を見る。
    std::tr1::unordered_map<std::string, StorageManager *> active_txns;
    PrintCpu("Worker", thread_id);

    MessageProto message;
    while (true)
    {
        // =====================================================
        // 1) 非同期 Read の返却（READ_RESULT）を先に処理
        // =====================================================
        if (scheduler->message_queues[thread_id]->Pop(&message))
        {
            assert(message.type() == MessageProto::READ_RESULT);
            const std::string &chan = message.destination_channel();
            StorageManager *manager = NULL;
            auto it = active_txns.find(chan);
            if (it != active_txns.end())
                manager = it->second;

            if (manager)
            {
                manager->HandleReadResult(message);
                if (manager->ReadyToExecute())
                {
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

        // =====================================================
        // 2) 新しい仕事の取得（RW優先 → 自スレッドRO → スティール）
        // =====================================================
        TxnProto *txn = NULL;

        // 2-1) まず RW キュー（非ブロッキング）
        if (!scheduler->rw_txns_queue_->Pop(&txn))
        {
            // 2-2) 取れなければ自分の RO キュー
            if (!scheduler->ro_queues_[thread_id]->Pop(&txn))
            {
                // 2-3) それでも無ければスティール（別ワーカの RO キュー）
                int victim_id = rand() % NUM_WORKERS;
                if (victim_id != thread_id)
                {
                    (void)scheduler->ro_queues_[victim_id]->Pop(&txn);
                }
            }
        }

        // 仕事が無ければ少し寝る
        if (txn == NULL)
        {
            usleep(100);
            continue;
        }

        // =====================================================
        // 3) 実行
        // =====================================================
        txn->set_time_worker_begin(GetTime());

        if (txn->read_only())
        {
            // -------- RO 高速パス：スナップショット（前エポック）を読む --------
            const int64_t snap_txn =
                txn->has_snapshot_txn_id()
                    ? txn->snapshot_txn_id()
                    : (static_cast<int64_t>(
                           std::max<uint64_t>(
                               scheduler->last_committed_batch_.load(std::memory_order_acquire), 1ULL) -
                           1ULL) *
                           MAX_LOCK_BATCH_SIZE +
                       (MAX_LOCK_BATCH_SIZE - 1));

            for (int i = 0; i < txn->read_set_size(); ++i)
            {
                (void)scheduler->storage_->ReadObject(txn->read_set(i), snap_txn);
            }
            for (int i = 0; i < txn->read_write_set_size(); ++i)
            {
                (void)scheduler->storage_->ReadObject(txn->read_write_set(i), snap_txn);
            }

            txn->set_time_worker_end(GetTime());
            scheduler->done_queue->Push(txn);
            continue;
        }

        // -------- RW 従来パス（ロック取得済: ready→rw_txns_queue_ から来たもの）--------
        StorageManager *manager = new StorageManager(
            scheduler->configuration_, scheduler->thread_connections_[thread_id],
            scheduler->storage_, txn);

        if (manager->ReadyToExecute())
        {
            scheduler->application_->Execute(txn, manager);
            txn->set_time_worker_end(GetTime());
            delete manager;
            scheduler->done_queue->Push(txn);
        }
        else
        {
            // 非同期 Read 経路：後続の READ_RESULT で続きが走る
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

void *DeterministicScheduler::LockManagerThread(void *arg)
{
    PrintCpu("Lock Manager", 0);
    DeterministicScheduler *scheduler = reinterpret_cast<DeterministicScheduler *>(arg);

    auto BatchOfTxn = [](int64_t txn_id) -> int
    {
        return static_cast<int>(txn_id / MAX_LOCK_BATCH_SIZE);
    };

    // バッチBに対応するtxn_id上限(cut)を公開
    auto PublishCutForBatch = [&](int B)
    {
        const int64 cut = static_cast<int64>(B) * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);
        scheduler->storage_->PublishSnapshot(cut);
    };

    // バッチ確定: PublishSnapshot → last_committed_batch_ advance
    auto AdvanceCommittedPrefixLocked = [&]()
    {
        for (;;)
        {
            auto it = scheduler->pending_rw_per_batch_.find(scheduler->next_batch_to_commit_);
            int pending = (it == scheduler->pending_rw_per_batch_.end()) ? 0 : it->second;
            if (pending == 0)
            {
                if (it != scheduler->pending_rw_per_batch_.end())
                {
                    scheduler->pending_rw_per_batch_.erase(it);
                }
                // 先にスナップショットを前進（prev <- curr, curr をBのcutへ）
                PublishCutForBatch(scheduler->next_batch_to_commit_);
                // その後、latest（=last_committed_batch_）をBに更新
                scheduler->last_committed_batch_.store(scheduler->next_batch_to_commit_, std::memory_order_release);
                scheduler->next_batch_to_commit_++;
            }
            else
            {
                break;
            }
        }
    };

    unordered_map<int, MessageProto *> batches;
    MessageProto *batch_message = NULL;
    int txns = 0;
    double time = GetTime();
    int pending_txns = 0;
    int batch_offset = 0;
    int batch_number = 0;

    // RWが来ない期間のバックオフ
    int empty_poll_streak = 0;
    const int kBackoffFloorUs = 200;
    const int kBackoffCeilUs = 5000;

    while (true)
    {
        // -------------------------------------------------
        // 1) ワーカー完了キューから回収
        // -------------------------------------------------
        TxnProto *done_txn;
        bool got_it = scheduler->done_queue->Pop(&done_txn);
        if (got_it)
        {
            empty_poll_streak = 0;

            if (done_txn->has_read_only() && done_txn->read_only())
            {
                // RO のメトリクス集計
                if (done_txn->has_time_sequencer_begin())
                {
                    double d = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();

                    double cur = scheduler->total_ro_dispatch_time_.load();
                    while (!scheduler->total_ro_dispatch_time_.compare_exchange_weak(cur, cur + d))
                    {
                    }
                    cur = scheduler->total_ro_queueing_time_.load();
                    while (!scheduler->total_ro_queueing_time_.compare_exchange_weak(cur, cur + q))
                    {
                    }
                    cur = scheduler->total_ro_worker_time_.load();
                    while (!scheduler->total_ro_worker_time_.compare_exchange_weak(cur, cur + w))
                    {
                    }
                    scheduler->processed_rot_count_++;
                }
            }
            else
            {
                // RW のロック解放とコミット前進
                scheduler->lock_manager_->Release(done_txn);
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    int B = BatchOfTxn(done_txn->txn_id());
                    auto it = scheduler->pending_rw_per_batch_.find(B);
                    if (it != scheduler->pending_rw_per_batch_.end() && --(it->second) < 0)
                        it->second = 0;
                    // ここでバッチ確定 → スナップショット公開 → latest更新
                    AdvanceCommittedPrefixLocked();
                }

                // RW のメトリクス集計
                if (done_txn->has_time_sequencer_begin())
                {
                    double seq = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();

                    double cur = scheduler->total_sequencer_time_.load();
                    while (!scheduler->total_sequencer_time_.compare_exchange_weak(cur, cur + seq))
                    {
                    }
                    cur = scheduler->total_queueing_time_.load();
                    while (!scheduler->total_queueing_time_.compare_exchange_weak(cur, cur + q))
                    {
                    }
                    cur = scheduler->total_worker_time_.load();
                    while (!scheduler->total_worker_time_.compare_exchange_weak(cur, cur + w))
                    {
                    }
                    scheduler->processed_rwt_count_++;
                }
            }

            scheduler->executing_txns_--;
            // ★ 常にインクリメント（確率的カウントを廃止）
            txns++;
            delete done_txn;
        }
        else
        {
            // -------------------------------------------------
            // 2) Sequencer からRW取り込み（ロック要求→readyへ）
            // -------------------------------------------------
            if (batch_message == NULL)
            {
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
                if (batch_message == NULL)
                {
                    empty_poll_streak = std::min(empty_poll_streak + 1, 1000000);
                    int sleep_us = std::min(kBackoffCeilUs, kBackoffFloorUs << std::min(empty_poll_streak, 8));
                    usleep(sleep_us);
                    goto FLUSH_READY_AND_PRINT;
                }
                else
                {
                    empty_poll_streak = 0;
                }
            }
            else if (batch_offset >= batch_message->data_size())
            {
                batch_offset = 0;
                batch_number++;
                delete batch_message;
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
                if (batch_message == NULL)
                {
                    empty_poll_streak = std::min(empty_poll_streak + 1, 1000000);
                    int sleep_us = std::min(kBackoffCeilUs, kBackoffFloorUs << std::min(empty_poll_streak, 8));
                    usleep(sleep_us);
                    goto FLUSH_READY_AND_PRINT;
                }
                else
                {
                    empty_poll_streak = 0;
                }
            }
            else if (pending_txns < MAX_ACTIVE_TXNS)
            {
                const int B = batch_message->batch_number();
                int enqueued = 0;
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    (void)scheduler->pending_rw_per_batch_[B]; // 無ければ0で作る
                }

                for (int i = 0; i < LOCK_BATCH_SIZE; i++)
                {
                    if (batch_offset >= batch_message->data_size())
                        break;

                    TxnProto *txn = new TxnProto();
                    txn->ParseFromString(batch_message->data(batch_offset));
                    batch_offset++;

                    txn->set_time_sequencer_begin(GetTime());
                    assert(!(txn->has_read_only() && txn->read_only()));

                    scheduler->lock_manager_->Lock(txn);
                    pending_txns++;
                    enqueued++;
                }

                if (enqueued > 0)
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    scheduler->pending_rw_per_batch_[B] += enqueued;
                    // 取り込み後にも前進可能性あり（空バッチなど）
                    AdvanceCommittedPrefixLocked();
                }
            }
        }

    FLUSH_READY_AND_PRINT:
        // -------------------------------------------------
        // 3) Ready な RW を実行キューへ
        // -------------------------------------------------
        while (!scheduler->ready_txns_->empty())
        {
            TxnProto *txn = scheduler->ready_txns_->front();
            scheduler->ready_txns_->pop_front();
            txn->set_time_sequencer_end(GetTime());
            pending_txns--;
            scheduler->executing_txns_++;
            scheduler->rw_txns_queue_->Push(txn);
        }

        // -------------------------------------------------
        // 4) 1秒毎に進捗表示＆メトリクスリセット
        // -------------------------------------------------
        if (GetTime() > time + 1)
        {
            double total_time = GetTime() - time;
            double tps = (total_time > 0.0) ? (static_cast<double>(txns) / total_time) : 0.0;

            int current_executing = scheduler->executing_txns_.load();
            // 人間向け
            std::cout << "Completed " << tps
                      << " txns/sec, " << current_executing
                      << " executing, " << pending_txns << " pending\n";
            // スクリプト向け（必ず1行出す）
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

            // リセット
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
