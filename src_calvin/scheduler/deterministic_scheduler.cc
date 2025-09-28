// Author: Kun Ren (kun@cs.yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// The deterministic lock manager implements deterministic locking as described
// in 'The Case for Determinism in Database Systems', VLDB 2010. Each
// transaction must request all locks it will ever need before the next
// transaction in the specified order may acquire any locks. Each lock is then
// granted to transactions in the order in which they requested them (i.e. in
// the global transaction order).
//
// TODO(scw): replace iostream with cstdio

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

using std::map;
using std::pair;
using std::string;
using std::tr1::unordered_map;
using zmq::socket_t;

// ZMQ用のヘルパー関数 (変更なし)
static void DeleteTxnPtr(void* data, void* hint) {
  free(data);
}

void DeterministicScheduler::SendTxnPtr(socket_t* socket, TxnProto* txn) {
  TxnProto** txn_ptr = reinterpret_cast<TxnProto**>(malloc(sizeof(txn)));
  *txn_ptr = txn;
  zmq::message_t msg(txn_ptr, sizeof(*txn_ptr), DeleteTxnPtr, NULL);
  socket->send(msg);
}

TxnProto* DeterministicScheduler::GetTxnPtr(socket_t* socket,
                                            zmq::message_t* msg) {
  if (!socket->recv(msg, ZMQ_NOBLOCK))
    return NULL;
  TxnProto* txn = *reinterpret_cast<TxnProto**>(msg->data());
  return txn;
}

// ★ 変更点: コンストラクタで新しいキューを初期化
DeterministicScheduler::DeterministicScheduler(Configuration* conf,
                                               Connection* batch_connection,
                                               Storage* storage,
                                               const Application* application)
    : configuration_(conf),
      batch_connection_(batch_connection),
      storage_(storage),
      application_(application) {
  ready_txns_ = new std::deque<TxnProto*>();
  lock_manager_ = new DeterministicLockManager(ready_txns_, configuration_);

  txns_queue = new AtomicQueue<TxnProto*>();
  done_queue = new AtomicQueue<TxnProto*>();
  
  // ★ 新しい入力キューを初期化
  sequencer_input_queue_ = new AtomicQueue<TxnProto*>();

  for (int i = 0; i < NUM_WORKERS; i++) {
    message_queues[i] = new AtomicQueue<MessageProto>();
  }

  Spin(1);

  // start lock manager thread
  cpu_set_t cpuset;
  pthread_attr_t attr1;
  pthread_attr_init(&attr1);
  CPU_ZERO(&cpuset);
  CPU_SET(LOCK_MANAGER_CORE, &cpuset);
  pthread_attr_setaffinity_np(&attr1, sizeof(cpu_set_t), &cpuset);
  pthread_create(&lock_manager_thread_, &attr1, LockManagerThread,
                 reinterpret_cast<void*>(this));

  // Start all worker threads.
  for (int i = 0; i < NUM_WORKERS; i++) {
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
                   reinterpret_cast<void*>(
                       new pair<int, DeterministicScheduler*>(i, this)));
  }
}

// ★ 新しいメソッドの実装
void DeterministicScheduler::PostTxn(TxnProto* txn) {
  sequencer_input_queue_->Push(txn);
}


// UnfetchAll と RunWorkerThread は変更なし
void UnfetchAll(Storage* storage, TxnProto* txn) {
  for (int i = 0; i < txn->read_set_size(); i++)
    if (StringToInt(txn->read_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_set(i));
  for (int i = 0; i < txn->read_write_set_size(); i++)
    if (StringToInt(txn->read_write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->read_write_set(i));
  for (int i = 0; i < txn->write_set_size(); i++)
    if (StringToInt(txn->write_set(i)) > COLD_CUTOFF)
      storage->Unfetch(txn->write_set(i));
}

void* DeterministicScheduler::RunWorkerThread(void* arg) {
  int thread =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->first;
  DeterministicScheduler* scheduler =
      reinterpret_cast<pair<int, DeterministicScheduler*>*>(arg)->second;

  unordered_map<string, StorageManager*> active_txns;
  PrintCpu("Worker", thread);

  MessageProto message;
  while (true) {
    bool got_message = scheduler->message_queues[thread]->Pop(&message);
    if (got_message == true) {
      assert(message.type() == MessageProto::READ_RESULT);
      StorageManager* manager = active_txns[message.destination_channel()];
      manager->HandleReadResult(message);
      if (manager->ReadyToExecute()) {
        TxnProto* txn = manager->txn_;
        scheduler->application_->Execute(txn, manager);
        delete manager;
        scheduler->thread_connections_[thread]->UnlinkChannel(IntToString(txn->txn_id()));
        active_txns.erase(message.destination_channel());
        scheduler->done_queue->Push(txn);
      }
    } else {
      TxnProto* txn;
      bool got_it = scheduler->txns_queue->Pop(&txn);
      if (got_it == true) {
        StorageManager* manager = new StorageManager(
            scheduler->configuration_, scheduler->thread_connections_[thread],
            scheduler->storage_, txn);
        if (manager->ReadyToExecute()) {
          scheduler->application_->Execute(txn, manager);
          delete manager;
          scheduler->done_queue->Push(txn);
        } else {
          scheduler->thread_connections_[thread]->LinkChannel(IntToString(txn->txn_id()));
          active_txns[IntToString(txn->txn_id())] = manager;
        }
      }
    }
  }
  return NULL;
}

DeterministicScheduler::~DeterministicScheduler() {}

// ★ 変更点: GetBatch関数は不要になったため完全に削除

// void* DeterministicScheduler::LockManagerThread(void* arg) { ... }
// ★ 変更点: LockManagerThreadのロジックを全面的に書き換え
void* DeterministicScheduler::LockManagerThread(void* arg) {
  PrintCpu("Lock Manager", 0);

  DeterministicScheduler* scheduler =
      reinterpret_cast<DeterministicScheduler*>(arg);

  int txns = 0;
  double time = GetTime();
  int executing_txns = 0;
  int pending_txns = 0;

  while (true) {
    TxnProto* done_txn;
    // 1. 完了したトランザクションを処理する
    if (scheduler->done_queue->Pop(&done_txn)) {
      executing_txns--;
      txns++;
      // ロックを解放
      scheduler->lock_manager_->Release(done_txn);
      // メモリ管理: Schedulerが責任を持ってdeleteする
      delete done_txn;
    }

    // 2. 処理能力に余裕があれば、Sequencerから新しいトランザクションを受け入れる
    if (executing_txns + pending_txns < MAX_ACTIVE_TXNS) {
      TxnProto* new_txn;
      if (scheduler->sequencer_input_queue_->Pop(&new_txn)) {
        // ネットワーク経由ではないので、デシリアライズは不要
        scheduler->lock_manager_->Lock(new_txn);
        pending_txns++;
      }
    }

    // 3. ロック獲得が完了し、実行準備ができたトランザクションをワーカースレッドに渡す
    while (!scheduler->ready_txns_->empty()) {
      TxnProto* ready_txn = scheduler->ready_txns_->front();
      scheduler->ready_txns_->pop_front();
      pending_txns--;
      executing_txns++;
      scheduler->txns_queue->Push(ready_txn);
    }

    // 4. スループットを1秒ごとに報告
    if (GetTime() > time + 1) {
      double total_time = GetTime() - time;
      std::cout << "Completed " << (static_cast<double>(txns) / total_time)
                << " txns/sec, "
                << executing_txns << " executing, " << pending_txns
                << " pending" << std::endl;
      time = GetTime();
      txns = 0;
    }
  }
  return NULL;
}