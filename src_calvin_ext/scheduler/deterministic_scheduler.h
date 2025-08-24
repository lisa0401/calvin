#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <deque>
#include <map>
#include <vector>
#include <mutex> // ★ 追加

#include "scheduler/scheduler.h"
#include "common/utils.h" // AtomicQueue はここにある
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

// ZMQ 前方宣言
namespace zmq
{
    class socket_t;
    class message_t;
}
using zmq::socket_t;

class Application;
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
                           std::vector<Connection *> *ro_connections,
                           Storage *storage,
                           const Application *application);
    virtual ~DeterministicScheduler();

    // RO が読む「最後にコミット済みのバッチ番号」
    std::atomic<uint64_t> last_committed_batch_{0};

    // RW コミット前缶詰数（バッチ単位）
    std::map<int, int> pending_rw_per_batch_;
    int next_batch_to_commit_ = 0;
    std::mutex pending_mu_;

private:
    friend class MockDeterministicScheduler;

    // スレッドエントリ
    static void *RunWorkerThread(void *arg);
    static void *LockManagerThread(void *arg);
    static void *RODispatcherThread(void *arg);

    // ZMQ 経由で TxnProto* を送受信（使うなら）
    void SendTxnPtr(zmq::socket_t *socket, TxnProto *txn);
    TxnProto *GetTxnPtr(zmq::socket_t *socket, zmq::message_t *msg);

    // 構成
    Configuration *configuration_;
    Connection *rw_connection_;
    std::vector<Connection *> *ro_connections_;
    Storage *storage_;
    const Application *application_;

    // 実行中トランザクション数（計測用）
    std::atomic<int> executing_txns_{0};

    // ★ RO 用ロックフリー・キュー（ワーカ毎）: AtomicQueue は common/utils.h の実装を使用
    AtomicQueue<TxnProto *> *ro_queues_[NUM_WORKERS];

    // RWキュー / 完了キュー
    std::deque<TxnProto *> *ready_txns_;
    DeterministicLockManager *lock_manager_;
    AtomicQueue<TxnProto *> *rw_txns_queue_;
    AtomicQueue<TxnProto *> *done_queue;
    AtomicQueue<MessageProto> *message_queues[NUM_WORKERS];
    Connection *thread_connections_[NUM_WORKERS];

    // スレッド
    pthread_t threads_[NUM_WORKERS];
    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_threads_[NUM_RO_DISPATCHERS];

    // ---- 計測（RW）----
    std::atomic<double> total_sequencer_time_{0};
    std::atomic<double> total_queueing_time_{0};
    std::atomic<double> total_worker_time_{0};
    std::atomic<int> processed_rwt_count_{0};

    // ---- 計測（RO）----
    std::atomic<double> total_ro_dispatch_time_{0};
    std::atomic<double> total_ro_queueing_time_{0};
    std::atomic<double> total_ro_worker_time_{0};
    std::atomic<int> processed_rot_count_{0};

    // ★ Dispatcher 全体で共有するラウンドロビン用チケット
    static std::atomic<uint64_t> ro_rr_ticket_;
};

#endif // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
