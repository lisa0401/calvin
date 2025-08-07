// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)

#include "scheduler/deterministic_scheduler.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <tr1/unordered_map>
#include <utility>
#include <sched.h>
#include <map>

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
            TxnProto *txn = new TxnProto();
            txn->ParseFromString(message.data(0));

            scheduler->executing_txns_++;

            uint64_t dispatch_count = scheduler->ro_dispatch_counter_++;
            int worker_id = dispatch_count % NUM_WORKERS;
            scheduler->ro_txns_queues_[worker_id]->Push(txn);
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
      ro_dispatch_counter_(0),
      executing_txns_(0)
{
    ready_txns_ = new std::deque<TxnProto *>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

    rw_txns_queue_ = new AtomicQueue<TxnProto *>();
    done_queue = new AtomicQueue<TxnProto *>();

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        ro_txns_queues_[i] = new AtomicQueue<TxnProto *>();
        message_queues[i] = new AtomicQueue<MessageProto>();
    }

    Spin(1);

    cpu_set_t cpuset;

    pthread_attr_t attr_ro_dispatcher;
    pthread_attr_init(&attr_ro_dispatcher);
    CPU_ZERO(&cpuset);
    CPU_SET(RO_DISPATCHER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_ro_dispatcher, sizeof(cpu_set_t), &cpuset);
    pthread_create(&ro_dispatcher_thread_, &attr_ro_dispatcher, RODispatcherThread,
                   reinterpret_cast<void *>(this));

    pthread_attr_t attr_lock_manager;
    pthread_attr_init(&attr_lock_manager);
    CPU_ZERO(&cpuset);
    CPU_SET(LOCK_MANAGER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_lock_manager, sizeof(cpu_set_t), &cpuset);
    pthread_create(&lock_manager_thread_, &attr_lock_manager, LockManagerThread,
                   reinterpret_cast<void *>(this));

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        string channel("scheduler");
        channel.append(IntToString(i));
        thread_connections_[i] = rw_connection_->multiplexer()->NewConnection(
            channel, &message_queues[i]);

        pthread_attr_t attr;
        pthread_attr_init(&attr);
        CPU_ZERO(&cpuset);
        CPU_SET(GET_WORKER_CORE(i), &cpuset);
        pthread_attr_setaffinity_np(&attr, sizeof(cpu_set_t), &cpuset);

        pthread_create(&(threads_[i]), &attr, RunWorkerThread,
                       reinterpret_cast<void *>(
                           new pair<int, DeterministicScheduler *>(i, this)));
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
        delete ro_txns_queues_[i];
        delete message_queues[i];
    }
}

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    int thread_id =
        reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->first;
    DeterministicScheduler *scheduler =
        reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->second;

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
                delete manager;
                scheduler->thread_connections_[thread_id]->UnlinkChannel(
                    IntToString(txn->txn_id()));
                active_txns.erase(message.destination_channel());
                scheduler->done_queue->Push(txn);
            }
        }
        else
        {
            TxnProto *txn;
            bool got_it = false;
            if (is_rw_worker)
            {
                if (scheduler->rw_txns_queue_->Pop(&txn))
                {
                    got_it = true;
                }
                else
                {
                    got_it = scheduler->ro_txns_queues_[thread_id]->Pop(&txn);
                }
            }
            else
            {
                got_it = scheduler->ro_txns_queues_[thread_id]->Pop(&txn);
            }

            if (got_it == true)
            {
                StorageManager *manager = new StorageManager(
                    scheduler->configuration_, scheduler->thread_connections_[thread_id],
                    scheduler->storage_, txn);
                if (manager->ReadyToExecute())
                {
                    scheduler->application_->Execute(txn, manager);
                    delete manager;
                    scheduler->done_queue->Push(txn);
                }
                else
                {
                    scheduler->thread_connections_[thread_id]->LinkChannel(
                        IntToString(txn->txn_id()));
                    active_txns[IntToString(txn->txn_id())] = manager;
                }
            }
        }
    }
    return NULL;
}

// ----------- ★★★ 修正箇所 (1/2) ★★★ -----------
// GetBatch関数はLockManagerThreadからのみ呼び出されるため、
// スレッドローカルな状態を持つように、引数としてbatchesマップを受け取るように変更
MessageProto *GetBatch(int batch_id, Connection *connection,
                       unordered_map<int, MessageProto *> *batches)
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
// ----------------------------------------------

void *DeterministicScheduler::LockManagerThread(void *arg)
{
    PrintCpu("Lock Manager", 0);

    DeterministicScheduler *scheduler =
        reinterpret_cast<DeterministicScheduler *>(arg);

    // ----------- ★★★ 修正箇所 (2/2) ★★★ -----------
    // グローバル変数だったbatchesを、このスレッド専用のローカル変数に変更
    unordered_map<int, MessageProto *> batches;
    // ----------------------------------------------

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
            if (!(done_txn->has_read_only() && done_txn->read_only()))
            {
                scheduler->lock_manager_->Release(done_txn);
            }
            scheduler->executing_txns_--;

            if (done_txn->writers_size() == 0 ||
                rand() % done_txn->writers_size() == 0)
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
            time = GetTime();
            txns = 0;
        }
    }
}