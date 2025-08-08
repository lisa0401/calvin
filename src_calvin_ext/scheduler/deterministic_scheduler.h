// scheduler/deterministic_scheduler.h (全文)

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <deque>
#include <vector>

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::deque;
using std::vector;

namespace zmq
{
    class socket_t;
    class message_t;
}
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
    Connection *rw_connection_;
    Connection *ro_connection_;
    Storage *storage_;
    const Application *application_;
    DeterministicLockManager *lock_manager_;

    pthread_t threads_[NUM_WORKERS];
    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_thread_;
    Connection *thread_connections_[NUM_WORKERS];

    // --- ★★★ 変更点 ★★★ ---
    std::deque<TxnProto *> *ready_txns_;
    AtomicQueue<TxnProto *> *rw_txns_queue_;
    AtomicQueue<TxnProto *> *shared_ro_queue_; // ROワーカー用の共有キューを1つだけ持つ
    AtomicQueue<TxnProto *> *done_queue;
    AtomicQueue<MessageProto> *message_queues[NUM_WORKERS];
    // ro_txns_queues_ 配列は不要になったため削除

    std::atomic<int> executing_txns_;
    // ro_dispatch_counter_ は不要になったため削除
    // -------------------------

    // レイテンシ測定用のメンバ変数
    std::atomic<double> total_sequencer_time_;
    std::atomic<double> total_queueing_time_;
    std::atomic<double> total_worker_time_;
    std::atomic<int> processed_rwt_count_;

    std::atomic<double> total_ro_dispatch_time_;
    std::atomic<double> total_ro_queueing_time_;
    std::atomic<double> total_ro_worker_time_;
    std::atomic<int> processed_rot_count_;
};

#endif // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_