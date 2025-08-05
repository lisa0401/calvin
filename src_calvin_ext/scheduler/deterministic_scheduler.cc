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
#include "applications/tpcc.h"

#include "sequencer/sequencer.h"
#include "common/debug.hh"
#include "backend/txn_proto_ext.h"

using std::map;
using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;

static void DeleteTxnPtr(void *data, void *hint)
{
    free(data);
}

void DeterministicScheduler::SendTxnPtr(socket_t *socket, TxnProtoExt *txn)
{
    TxnProtoExt **txn_ptr = reinterpret_cast<TxnProtoExt **>(malloc(sizeof(txn)));
    *txn_ptr = txn;
    zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
    socket->send(msg);
}

TxnProtoExt *DeterministicScheduler::GetTxnPtr(socket_t *socket,
                                               zmq::message_t *msg)
{
    if (!socket->recv(msg, ZMQ_NOBLOCK))
        return NULL;
    TxnProtoExt *base_txn = *reinterpret_cast<TxnProtoExt **>(msg->data());

    return base_txn;
}

DeterministicScheduler::DeterministicScheduler(Configuration *conf,
                                               Connection *batch_connection,
                                               Storage *storage,
                                               const Application *application)
    : configuration_(conf),
      batch_connection_(batch_connection),
      storage_(storage),
      application_(application)
{
    ready_txns_ = new std::deque<TxnProtoExt *>();
    lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

    txns_queue = new AtomicQueue<TxnProtoExt *>();
    done_queue = new AtomicQueue<TxnProtoExt *>();

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        message_queues[i] = new AtomicQueue<MessageProto>();
    }

    Spin(1);

    cpu_set_t cpuset;
    pthread_attr_t attr1;
    pthread_attr_init(&attr1);

    CPU_ZERO(&cpuset);
    CPU_SET(LOCK_MANAGER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset);
    pthread_create(&lock_manager_thread_, &attr1, LockManagerThread,
                   reinterpret_cast<void *>(this));

    for (int i = 0; i < NUM_WORKERS; i++)
    {
        string channel("scheduler");
        channel.append(IntToString(i));
        thread_connections_[i] = batch_connection_->multiplexer()->NewConnection(
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

void *DeterministicScheduler::RunWorkerThread(void *arg)
{
    int thread =
        reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->first;
    DeterministicScheduler *scheduler =
        reinterpret_cast<pair<int, DeterministicScheduler *> *>(arg)->second;

    unordered_map<string, StorageManager *> active_txns;

    PrintCpu("Worker", thread);

    MessageProto message;
    while (true)
    {
        bool got_message = scheduler->message_queues[thread]->Pop(&message);
        if (got_message == true)
        {
            assert(message.type() == MessageProto::READ_RESULT);
            StorageManager *manager = active_txns[message.destination_channel()];
            manager->HandleReadResult(message);
            if (manager->ReadyToExecute())
            {
                TxnProtoExt *txn = manager->txn_;

                // 実行とバリデーションを`TxnProtoExt::Execute`に一任
                if (txn->Execute(manager, scheduler->application_))
                {
                    // バリデーション成功
                    txn->set_status(TxnProto::COMMITTED);
                    scheduler->done_queue->Push(txn);
                }
                else
                {
                    // バリデーション失敗
                    txn->set_status(TxnProto::ABORTED);
                    scheduler->done_queue->Push(txn);
                }

                delete manager;
                scheduler->thread_connections_[thread]->UnlinkChannel(IntToString(txn->txn_id()));
                active_txns.erase(message.destination_channel());
            }
        }
        else
        {
            TxnProtoExt *txn;
            bool got_it = scheduler->txns_queue->Pop(&txn);
            if (got_it == true)
            {
                StorageManager *manager = new StorageManager(
                    scheduler->configuration_, scheduler->thread_connections_[thread],
                    scheduler->storage_, txn);

                if (manager->ReadyToExecute())
                {
                    // 実行とバリデーションを`TxnProtoExt::Execute`に一任
                    if (txn->Execute(manager, scheduler->application_))
                    {
                        // バリデーション成功
                        txn->set_status(TxnProto::COMMITTED);
                        scheduler->done_queue->Push(txn);
                    }
                    else
                    {
                        // バリデーション失敗
                        txn->set_status(TxnProto::ABORTED);
                        scheduler->done_queue->Push(txn);
                    }

                    delete manager;
                }
                else
                {
                    scheduler->thread_connections_[thread]->LinkChannel(
                        IntToString(txn->txn_id()));
                    active_txns[IntToString(txn->txn_id())] = manager;
                }
            }
        }
    }
    return NULL;
}
DeterministicScheduler::~DeterministicScheduler() {}

unordered_map<int, MessageProto *> batches;
MessageProto *GetBatch(int batch_id, Connection *connection)
{
    if (batches.count(batch_id) > 0)
    {
        MessageProto *batch = batches[batch_id];
        batches.erase(batch_id);
        return batch;
    }
    else
    {
        MessageProto *message = new MessageProto();
        if (connection->GetMessage(message))
        {
            assert(message->type() == MessageProto::TXN_BATCH);
            if (message->batch_number() == batch_id)
            {
                return message;
            }
            else
            {
                batches[message->batch_number()] = message;
            }
        }
        else
        {
            delete message;
        }
        return NULL;
    }
}

void *DeterministicScheduler::LockManagerThread(void *arg)
{
    PrintCpu("Lock Manager", 0);

    DeterministicScheduler *scheduler =
        reinterpret_cast<DeterministicScheduler *>(arg);

    MessageProto *batch_message = NULL;
    int txns = 0;
    double time = GetTime();
    int executing_txns = 0;
    int pending_txns = 0;
    int batch_offset = 0;
    int batch_number = 0;

    int tasks[Task::Size] = {0};
    std::string task_names[Task::Size] = {"ProcessDoneTransaction",
                                          "LoadNextBatch", "AdvanceBatch",
                                          "Locking", "ProcessReadyTransaction"};

    while (true)
    {
        TxnProtoExt *done_txn;
        while (scheduler->done_queue->Pop(&done_txn))
        {
            executing_txns--;
            tasks[Task::ProcessDoneTransaction]++;

            if (done_txn->status() == TxnProto::COMMITTED)
            {
                // 成功の場合: 通常通りロックを解放し、トランザクションを破棄
                if (done_txn->writers_size() == 0 ||
                    rand() % done_txn->writers_size() == 0)
                    txns++;
                scheduler->lock_manager_->Release(done_txn);
                delete done_txn;
            }
            else
            { // status == ABORTED
                // 失敗の場合: ロックを解放し、シーケンサに送り返す
                scheduler->lock_manager_->Release(done_txn);

                // メッセージを作成してsequencerに送信
                MessageProto retry_msg;
                retry_msg.set_type(MessageProto::TXN_RETRY);
                retry_msg.add_data(done_txn->SerializeAsString());

                // sequencerへの通信先を指定（例）
                // 実際の実装に応じて、`sequencer_node_id`や`sequencer_channel`を変更してください
                retry_msg.set_destination_node(scheduler->configuration_->sequencer_node_id());
                retry_msg.set_destination_channel("sequencer_channel");

                scheduler->batch_connection_->Send(retry_msg);

                // 送信後は、このオブジェクトはsequencerが管理するので、ローカルで破棄
                delete done_txn;
            }
        }

        if (batch_message == NULL)
        {
            batch_message = GetBatch(batch_number, scheduler->batch_connection_);
            if (batch_message != NULL)
                tasks[Task::LoadNextBatch] += batch_message->data_size();
        }

        if (batch_message != NULL && batch_offset >= batch_message->data_size())
        {
            batch_offset = 0;
            batch_number++;
            delete batch_message;
            batch_message = NULL;
            tasks[Task::AdvanceBatch]++;
        }

        if (batch_message != NULL && (executing_txns + pending_txns < MAX_ACTIVE_TXNS))
        {
            for (int i = 0; i < LOCK_BATCH_SIZE; i++)
            {
                if (batch_offset >= batch_message->data_size())
                {
                    break;
                }
                TxnProtoExt *txn = new TxnProtoExt();
                txn->ParseFromString(batch_message->data(batch_offset));
                batch_offset++;

                // --- ここから修正 ---
                // read-onlyトランザクションを識別し、LockManagerをバイパスする
                if (txn->is_read_only())
                {
                    scheduler->txns_queue->Push(txn);
                    executing_txns++; // ロックマネージャースレッドが直接ワーカーキューにプッシュするので、executing_txnsをインクリメント
                    tasks[Task::ProcessReadyTransaction]++;
                }
                else
                {
                    scheduler->lock_manager_->Lock(txn);
                    pending_txns++;
                    tasks[Task::Locking]++;
                }
                // --- ここまで修正 ---
            }
        }

        while (!scheduler->ready_txns_->empty())
        {
            TxnProtoExt *txn = scheduler->ready_txns_->front();
            scheduler->ready_txns_->pop_front();
            pending_txns--;
            executing_txns++;

            scheduler->txns_queue->Push(txn);
            tasks[Task::ProcessReadyTransaction]++;
        }

        if (GetTime() > time + 1)
        {
            double total_time = GetTime() - time;
            std::string task_output = "Tasks: ";
            for (int i = 0; i < Task::Size; i++)
            {
                task_output.append(task_names[i] + ": " + std::to_string(tasks[i]) + ", ");
            }
            std::cout << "Completed " << (static_cast<double>(txns) / total_time)
                      << " txns/sec, " << executing_txns << " executing, " << pending_txns
                      << " pending, "
                      << "\n"
                      << task_output << "\n"
                      << std::flush;
            time = GetTime();
            txns = 0;
            memset(tasks, 0, sizeof(tasks));
        }
    }
    return NULL;
}