// scheduler/deterministic_scheduler.cc (全文)

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>
#include <vector>
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

void *DeterministicScheduler::RODispatcherThread(void *arg)
{
    DeterministicScheduler *scheduler = reinterpret_cast<DeterministicScheduler *>(arg);
    PrintCpu("RO Dispatcher", 0);

    MessageProto message;
    while (true)
    {
        if (scheduler->ro_connection_->GetMessage(&message))
        {
            assert(message.type() == MessageProto::TXN_BATCH);

            // ★★★ 変更点：バッチを解析し、個別のTxnを共有キューに投入 ★★★
            double batch_recv_time = GetTime();
            for (int i = 0; i < message.data_size(); i++)
            {
                TxnProto *txn = new TxnProto();
                txn->ParseFromString(message.data(i));

                txn->set_time_sequencer_begin(batch_recv_time);
                txn->set_time_sequencer_end(GetTime()); // 個別TxnをPushする時刻を記録

                scheduler->executing_txns_++;
                scheduler->shared_ro_queue_->Push(txn);
            }
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

DeterministicScheduler::DeterministicScheduler(Configuration *conf,
                                               Connection *rw_connection,
                                               Connection *ro_connection,
                                               Storage *storage,
                                               const Application *application)
    : configuration_(conf),
      rw_connection_(rw_connection),
      ro_connection_(ro_connection),
      storage_(storage),
      application_(application),
      executing_txns_(0)
{
    ready_txns_ = new std::deque<TxnProto *>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

    // ★★★ 変更点：キューの初期化方法を変更 ★★★
    rw_txns_queue_ = new AtomicQueue<TxnProto *>();
    shared_ro_queue_ = new AtomicQueue<TxnProto *>(); // 共有キューを初期化
    done_queue = new AtomicQueue<TxnProto *>();

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        message_queues[i] = new AtomicQueue<MessageProto>();
    }
    // ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★

    // 全ての測定用変数を初期化
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
    pthread_attr_t attr_ro_dispatcher;
    pthread_attr_init(&attr_ro_dispatcher);
    CPU_ZERO(&cpuset);
    CPU_SET(RO_DISPATCHER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_ro_dispatcher, sizeof(cpu_set_t), &cpuset);
    pthread_create(&ro_dispatcher_thread_, &attr_ro_dispatcher, RODispatcherThread, reinterpret_cast<void *>(this));

    pthread_attr_t attr_lock_manager;
    pthread_attr_init(&attr_lock_manager);
    CPU_ZERO(&cpuset);
    CPU_SET(LOCK_MANAGER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_lock_manager, sizeof(cpu_set_t), &cpuset);
    pthread_create(&lock_manager_thread_, &attr_lock_manager, LockManagerThread, reinterpret_cast<void *>(this));

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
    delete shared_ro_queue_; // 共有キューを解放
    for (int i = 0; i < NUM_WORKERS; i++)
    {
        delete message_queues[i];
    }
}

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    int thread_id = reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->first;
    DeterministicScheduler *scheduler = reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->second;

    const int NUM_RW_WORKERS = 1;
    bool is_rw_worker = (thread_id < NUM_RW_WORKERS);

    unordered_map<string, StorageManager *> active_txns;
    PrintCpu("Worker", thread_id);

    MessageProto message;
    while (true)
    {
        bool got_message = scheduler->message_queues[thread_id]->Pop(&message);
        if (got_message == true)
        {
            assert(message.type() == MessageProto::READ_RESULT);
            StorageManager *manager = active_txns[message.destination_channel()];
            manager->HandleReadResult(message);
            if (manager->ReadyToExecute())
            {
                TxnProto *txn = manager->txn_;
                scheduler->application_->Execute(txn, manager);
                txn->set_time_worker_end(GetTime());
                delete manager;
                scheduler->thread_connections_[thread_id]->UnlinkChannel(IntToString(txn->txn_id()));
                active_txns.erase(message.destination_channel());
                scheduler->done_queue->Push(txn);
            }
        }
        else
        {
            // ★★★ 変更点：共有キューから仕事を取得するロジック ★★★
            TxnProto *txn = NULL;
            bool got_txn = false;

            if (is_rw_worker)
            {
                // RWワーカーはまずRWキューを試す
                if (scheduler->rw_txns_queue_->Pop(&txn))
                {
                    got_txn = true;
                }
            }

            // RWの仕事がなかった場合、またはROワーカーの場合、共有ROキューを試す
            if (!got_txn)
            {
                if (scheduler->shared_ro_queue_->Pop(&txn))
                {
                    got_txn = true;
                }
            }

            if (got_txn)
            {
                // トランザクションを処理（RWでもROでも共通）
                txn->set_time_worker_begin(GetTime());
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
                    scheduler->thread_connections_[thread_id]->LinkChannel(IntToString(txn->txn_id()));
                    active_txns[IntToString(txn->txn_id())] = manager;
                }
            }
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
    unordered_map<int, MessageProto *> batches;
    MessageProto *batch_message = NULL;
    int txns = 0;
    double time = GetTime();
    int pending_txns = 0;
    int batch_offset = 0;
    int batch_number = 0;
    while (true)
    {
        TxnProto *done_txn;
        bool got_it = scheduler->done_queue->Pop(&done_txn);
        if (got_it == true)
        {
            if (done_txn->has_read_only() && done_txn->read_only())
            {
                if (done_txn->has_time_sequencer_begin())
                {
                    double dispatch_time = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double current_d_time = scheduler->total_ro_dispatch_time_.load();
                    while (!scheduler->total_ro_dispatch_time_.compare_exchange_weak(current_d_time, current_d_time + dispatch_time))
                    {
                    }
                    double q_time = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double current_q_time = scheduler->total_ro_queueing_time_.load();
                    while (!scheduler->total_ro_queueing_time_.compare_exchange_weak(current_q_time, current_q_time + q_time))
                    {
                    }
                    double w_time = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double current_w_time = scheduler->total_ro_worker_time_.load();
                    while (!scheduler->total_ro_worker_time_.compare_exchange_weak(current_w_time, current_w_time + w_time))
                    {
                    }
                    scheduler->processed_rot_count_++;
                }
            }
            else
            {
                scheduler->lock_manager_->Release(done_txn);
                if (done_txn->has_time_sequencer_begin())
                {
                    double seq_time = done_txn->time_sequencer_end() - done_txn->time_sequencer_begin();
                    double current_s_time = scheduler->total_sequencer_time_.load();
                    while (!scheduler->total_sequencer_time_.compare_exchange_weak(current_s_time, current_s_time + seq_time))
                    {
                    }
                    double q_time = done_txn->time_worker_begin() - done_txn->time_sequencer_end();
                    double current_q_time = scheduler->total_queueing_time_.load();
                    while (!scheduler->total_queueing_time_.compare_exchange_weak(current_q_time, current_q_time + q_time))
                    {
                    }
                    double w_time = done_txn->time_worker_end() - done_txn->time_worker_begin();
                    double current_w_time = scheduler->total_worker_time_.load();
                    while (!scheduler->total_worker_time_.compare_exchange_weak(current_w_time, current_w_time + w_time))
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
            if (batch_message == NULL)
            {
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
            }
            else if (batch_message != NULL && batch_offset >= batch_message->data_size())
            {
                batch_offset = 0;
                batch_number++;
                delete batch_message;
                batch_message = GetBatch(batch_number, scheduler->rw_connection_, &batches);
            }
            else if (batch_message != NULL && pending_txns < MAX_ACTIVE_TXNS)
            {
                for (int i = 0; i < LOCK_BATCH_SIZE; i++)
                {
                    if (batch_offset >= batch_message->data_size())
                    {
                        break;
                    }
                    TxnProto *txn = new TxnProto();
                    txn->ParseFromString(batch_message->data(batch_offset));
                    batch_offset++;
                    txn->set_time_sequencer_begin(GetTime());
                    assert(!(txn->has_read_only() && txn->read_only()));
                    scheduler->lock_manager_->Lock(txn);
                    pending_txns++;
                }
            }
        }
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
                      << " txns/sec, "
                      << current_executing << " executing, " << pending_txns
                      << " pending\n"
                      << std::flush;
            int processed_rwt = scheduler->processed_rwt_count_.load();
            if (processed_rwt > 0)
            {
                double avg_seq = scheduler->total_sequencer_time_.load() / processed_rwt;
                double avg_q = scheduler->total_queueing_time_.load() / processed_rwt;
                double avg_w = scheduler->total_worker_time_.load() / processed_rwt;
                std::cout << "-------------------- RW Latency (ms) -------------------\n"
                          << "  Sequencer: " << avg_seq * 1000 << " | Queueing: " << avg_q * 1000
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
                          << "  Dispatcher: " << avg_d * 1000 << " | Queueing: " << avg_q * 1000
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