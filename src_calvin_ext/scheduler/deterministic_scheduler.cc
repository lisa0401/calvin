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

static void DeleteTxnPtr(void *data, void *hint)
{
    free(data);
}

// RODispatcherThread: SequencerからTxnProto*を直接受け取る修正版
void *DeterministicScheduler::RODispatcherThread(void *arg)
{
    using PairT = pair<int, DeterministicScheduler *>;
    PairT *args = reinterpret_cast<PairT *>(arg);
    const int dispatcher_id = args->first;
    DeterministicScheduler *scheduler = args->second;
    delete args; // 引数用に確保されたメモリを解放

    PrintCpu("RO Dispatcher", dispatcher_id);

    // 受信バッチをワーカ別に一時集約
    std::vector<std::vector<TxnProto *>> local_batches(NUM_WORKERS);

    // ワーカ選択ロジック（変更なし）
    auto choose_worker = [](const TxnProto &t) -> uint64_t
    {
        if (t.read_set_size() > 0)
        {
            const std::string k = t.read_set(0);
            return static_cast<uint64_t>(std::hash<std::string>{}(k));
        }
        if (t.read_write_set_size() > 0)
        {
            const std::string k = t.read_write_set(0);
            return static_cast<uint64_t>(std::hash<std::string>{}(k));
        }
        return static_cast<uint64_t>(t.txn_id());
    };

    MessageProto message;
    while (true)
    {
        // 自身のIDに対応するConnectionからメッセージを受信
        if ((*scheduler->ro_connections_)[dispatcher_id]->GetMessage(&message))
        {
            assert(message.type() == MessageProto::TXN_BATCH);

            // --- ① 仕分けフェーズ ---
            const double batch_recv_time = GetTime();
            const uint64_t snap_epoch = scheduler->last_committed_batch_.load(std::memory_order_acquire);
            const int64_t snap_txnid =
                static_cast<int64_t>(snap_epoch) * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);

            // ★ 変更点 1: data_ptr_size() をループ条件にする
            for (int i = 0; i < message.data_ptr_size(); i++)
            {
                // ★ 変更点 2: new/ParseFromString をやめ、ポインタを復元する
                auto raw_ptr = message.data_ptr(i);
                TxnProto *txn = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw_ptr));

                // （計測）Dispatcher到着〜仕分けのタイムスタンプ
                txn->set_time_sequencer_begin(batch_recv_time);
                txn->set_time_sequencer_end(GetTime());

                // RO 以外が紛れ込んでいたらスキップ（保護的に）
                // ★ 変更点 3: ポインタを渡された側はdeleteしない。所有権はWorkerへ
                if (!(txn->has_read_only() && txn->read_only()))
                {
                    // 本来ここに来ない想定。もし来た場合、このtxnのメモリ解放をどうするかは
                    // システムの規約次第（捨てるならdelete、RW経路に回すならそのまま渡す）
                    continue;
                }

                // ★ スナップショット境界を付与（これが SI の肝）★
                txn->set_snapshot_epoch(snap_epoch);
                txn->set_snapshot_txn_id(snap_txnid);

                scheduler->executing_txns_++; // 実行中カウント（RO/RW共通）

                // ワーカ決定（NUM_WORKERS で剰余）
                uint64_t dest_worker = choose_worker(*txn) % NUM_WORKERS;
                local_batches[dest_worker].push_back(txn);
            }

            // --- ② 一括投入フェーズ（変更なし） ---
            for (uint64_t w = 0; w < NUM_WORKERS; ++w)
            {
                if (!local_batches[w].empty())
                {
                    std::lock_guard<std::mutex> lk(scheduler->worker_ro_queues_[w].m);
                    auto &dq = scheduler->worker_ro_queues_[w].q;
                    dq.insert(dq.end(), local_batches[w].begin(), local_batches[w].end());
                    local_batches[w].clear();
                }
            }
        }
        else
        {
            usleep(50);
        }
    }
    return NULL;
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
                                               vector<Connection *> *ro_connections,
                                               Storage *storage,
                                               const Application *application)
    : configuration_(conf),
      rw_connection_(rw_connection),
      ro_connections_(ro_connections),
      storage_(storage),
      application_(application),
      executing_txns_(0),
      worker_ro_queues_(NUM_WORKERS)
{
    ready_txns_ = new std::deque<TxnProto *>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

    rw_txns_queue_ = new AtomicQueue<TxnProto *>();
    done_queue = new AtomicQueue<TxnProto *>();

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        message_queues[i] = new AtomicQueue<MessageProto>();
    }

    // (測定用変数の初期化は変更なし)
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

    // 複数のRODispatcherスレッドを生成
    for (int i = 0; i < NUM_RO_DISPATCHERS; i++)
    {
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_RO_DISPATCHER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

        pair<int, DeterministicScheduler *> *arg = new pair<int, DeterministicScheduler *>(i, this);
        pthread_create(&(ro_dispatcher_threads_[i]), &attr, RODispatcherThread, reinterpret_cast<void *>(arg));
    }

    // LockManagerスレッド生成 (変更なし)
    pthread_attr_t attr_lock_manager;
    pthread_attr_init(&attr_lock_manager);
    CPU_ZERO(&cpuset);
    CPU_SET(LOCK_MANAGER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_lock_manager, sizeof(cpu_set_t), &cpuset);
    pthread_create(&lock_manager_thread_, &attr_lock_manager, LockManagerThread, reinterpret_cast<void *>(this));

    // Workerスレッド生成 (変更なし)
    for (int i = 0; i < NUM_WORKERS; i++)
    {
        string channel("scheduler");
        channel.append(IntToString(i));
        thread_connections_[i] = rw_connection_->multiplexer()->NewConnection(channel, &message_queues[i]);
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_WORKER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);
        pthread_create(&(threads_[i]), &attr, RunWorkerThread, reinterpret_cast<void *>(new pair<int, DeterministicScheduler *>(i, this)));
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
}

// RunWorkerThread (最終修正版)

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    // ----- 初期化 -----
    typedef std::pair<int, DeterministicScheduler *> pair_t;
    pair_t *args_pair = reinterpret_cast<pair_t *>(arg);
    const int thread_id = args_pair->first;
    DeterministicScheduler *scheduler = args_pair->second;
    delete args_pair;

    // RW ワーカーの本数（必要に応じて調整可）
    const int NUM_RW_WORKERS = 1;
    const bool is_rw_worker = (thread_id < NUM_RW_WORKERS);

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
            {
                std::tr1::unordered_map<std::string, StorageManager *>::iterator it = active_txns.find(chan);
                if (it != active_txns.end())
                    manager = it->second;
            }
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

        // 2-1) RW キュー（RW ワーカーのみ）
        if (is_rw_worker)
        {
            (void)scheduler->rw_txns_queue_->Pop(&txn);
        }
        // 2-2) 自分の RO キュー
        if (txn == NULL)
        {
            std::lock_guard<std::mutex> lk(scheduler->worker_ro_queues_[thread_id].m);
            if (!scheduler->worker_ro_queues_[thread_id].q.empty())
            {
                txn = scheduler->worker_ro_queues_[thread_id].q.back();
                scheduler->worker_ro_queues_[thread_id].q.pop_back();
            }
        }
        // 2-3) ワークスティーリング
        if (txn == NULL)
        {
            int victim_id = rand() % NUM_WORKERS;
            if (victim_id != thread_id)
            {
                std::unique_lock<std::mutex> lk(scheduler->worker_ro_queues_[victim_id].m, std::try_to_lock);
                if (lk.owns_lock() && !scheduler->worker_ro_queues_[victim_id].q.empty())
                {
                    txn = scheduler->worker_ro_queues_[victim_id].q.front();
                    scheduler->worker_ro_queues_[victim_id].q.pop_front();
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
            // -------- RO 高速パス：コミット済みスナップショットを読む --------
            // snapshot_txn_id が付与されていればそれを使用。無ければ last_committed_batch_ から計算。
            int64_t snap_txn =
                txn->has_snapshot_txn_id()
                    ? txn->snapshot_txn_id()
                    : (int64_t)scheduler->last_committed_batch_.load(std::memory_order_acquire) * MAX_LOCK_BATCH_SIZE + (MAX_LOCK_BATCH_SIZE - 1);

            for (int i = 0; i < txn->read_set_size(); ++i)
            {
                // CollapsedVersionedStorage::ReadObject(key, txn_id) を利用（戻り値は未使用でOK）
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

        // -------- RW 従来パス（ロック取得 → 非同期 Read → 実行）--------
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
            // 非同期 Read 経路：後続の READ_RESULT 処理で回収
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
    auto AdvanceCommittedPrefixLocked = [&]()
    {
        for (;;)
        {
            auto it = scheduler->pending_rw_per_batch_.find(scheduler->next_batch_to_commit_);
            int pending = (it == scheduler->pending_rw_per_batch_.end()) ? 0 : it->second;
            if (pending == 0)
            {
                if (it != scheduler->pending_rw_per_batch_.end())
                    scheduler->pending_rw_per_batch_.erase(it);
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

    // ★ 追加: NULL が続く時に退避するためのバックオフ
    int empty_poll_streak = 0;
    const int kBackoffFloorUs = 200; // 最小スリープ
    const int kBackoffCeilUs = 5000; // 最大スリープ

    while (true)
    {
        TxnProto *done_txn;
        bool got_it = scheduler->done_queue->Pop(&done_txn);
        if (got_it)
        {
            empty_poll_streak = 0; // 仕事が来たのでリセット

            if (done_txn->has_read_only() && done_txn->read_only())
            {
                if (done_txn->has_time_sequencer_begin())
                {
                    double dispatch_time = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double cur = scheduler->total_ro_dispatch_time_.load();
                    while (!scheduler->total_ro_dispatch_time_.compare_exchange_weak(cur, cur + dispatch_time))
                    {
                    }
                    double q_time = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    cur = scheduler->total_ro_queueing_time_.load();
                    while (!scheduler->total_ro_queueing_time_.compare_exchange_weak(cur, cur + q_time))
                    {
                    }
                    double w_time = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    cur = scheduler->total_ro_worker_time_.load();
                    while (!scheduler->total_ro_worker_time_.compare_exchange_weak(cur, cur + w_time))
                    {
                    }
                    scheduler->processed_rot_count_++;
                }
            }
            else
            {
                scheduler->lock_manager_->Release(done_txn);
                {
                    std::lock_guard<std::mutex> lk(scheduler->pending_mu_);
                    int B = BatchOfTxn(done_txn->txn_id());
                    auto it = scheduler->pending_rw_per_batch_.find(B);
                    if (it != scheduler->pending_rw_per_batch_.end() && --(it->second) < 0)
                        it->second = 0;
                    AdvanceCommittedPrefixLocked();
                }
                if (done_txn->has_time_sequencer_begin())
                {
                    double seq = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double cur = scheduler->total_sequencer_time_.load();
                    while (!scheduler->total_sequencer_time_.compare_exchange_weak(cur, cur + seq))
                    {
                    }
                    double q = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    cur = scheduler->total_queueing_time_.load();
                    while (!scheduler->total_queueing_time_.compare_exchange_weak(cur, cur + q))
                    {
                    }
                    double w = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    cur = scheduler->total_worker_time_.load();
                    while (!scheduler->total_worker_time_.compare_exchange_weak(cur, cur + w))
                    {
                    }
                    scheduler->processed_rwt_count_++;
                }
            }

            scheduler->executing_txns_--;
            if (done_txn->writers_size() == 0 || rand() % done_txn->writers_size() == 0)
                txns++;
            delete done_txn;
        }
        else
        {
            // === ここから RW の取り込み ===
            if (batch_message == NULL)
            {
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);

                if (batch_message == NULL)
                {
                    // ★ 追加: RW が全く来ていない＝ROオンリー期の可能性 → バックオフ
                    empty_poll_streak = std::min(empty_poll_streak + 1, 1000000);
                    int sleep_us = std::min(kBackoffCeilUs, kBackoffFloorUs << std::min(empty_poll_streak, 8));
                    // ただし ready_txns_ の排出があるかもしれないので短時間だけ寝る
                    usleep(sleep_us);
                    // 以降の処理をスキップして while 先頭へ（スピン防止）
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
                    AdvanceCommittedPrefixLocked();
                }
            }
        }

    FLUSH_READY_AND_PRINT:
        // Ready な RW を実行キューへ
        while (!scheduler->ready_txns_->empty())
        {
            TxnProto *txn = scheduler->ready_txns_->front();
            scheduler->ready_txns_->pop_front();
            txn->set_time_sequencer_end(GetTime());
            pending_txns--;
            scheduler->executing_txns_++;
            scheduler->rw_txns_queue_->Push(txn);
        }

        if (GetTime() > time + 1)
        {
            double total_time = GetTime() - time;
            int current_executing = scheduler->executing_txns_.load();
            std::cout << "Completed " << (static_cast<double>(txns) / total_time)
                      << " txns/sec, " << current_executing
                      << " executing, " << pending_txns << " pending\n"
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
