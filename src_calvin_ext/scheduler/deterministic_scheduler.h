// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <deque>

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::deque;

namespace zmq
{
    class socket_t;
    class message_t;
} // namespace zmq
using zmq::socket_t;

class Configuration;
class Connection;
class DeterministicLockManager;
class Storage;
class TxnProto;

class DeterministicScheduler : public Scheduler
{
public:
    DeterministicScheduler(Configuration *conf,
                           Connection *rw_connection,
                           Connection *ro_connection,
                           Storage *storage,
                           const Application *application);

    virtual ~DeterministicScheduler();

private:
    static void *RunWorkerThread(void *arg);
    static void *LockManagerThread(void *arg);
    static void *RODispatcherThread(void *arg);

    void SendTxnPtr(socket_t *socket, TxnProto *txn);
    TxnProto *GetTxnPtr(socket_t *socket, zmq::message_t *msg);

    Configuration *configuration_;

    pthread_t threads_[NUM_WORKERS];
    Connection *thread_connections_[NUM_WORKERS];

    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_thread_;

    Connection *rw_connection_;
    Connection *ro_connection_;

    Storage *storage_;
    const Application *application_;
    DeterministicLockManager *lock_manager_;
    std::deque<TxnProto *> *ready_txns_;

    // ----------- ★★★ 修正箇所 ★★★ -----------
    // ROトランザクション用のキューを、ワーカーごとに専用のものを持つ配列に変更
    AtomicQueue<TxnProto *> *ro_txns_queues_[NUM_WORKERS];
    // ROディスパッチャがラウンドロビンで振り分けるためのカウンタ
    std::atomic<uint64_t> ro_dispatch_counter_;
    // -----------------------------------------

    // R/Wキューは従来通り1つ
    AtomicQueue<TxnProto *> *rw_txns_queue_;
    AtomicQueue<TxnProto *> *done_queue;
    AtomicQueue<MessageProto> *message_queues[NUM_WORKERS];

    std::atomic<int> executing_txns_;
};
#endif // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_