// scheduler/deterministic_scheduler.h (偽共有対策・最終版)

#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <deque>
#include <vector>
#include <mutex>

#include "scheduler/scheduler.h"
#include "common/utils.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

using std::deque;
using std::mutex;
using std::vector;

// (zmqや他の前方宣言は変更なし)
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

// ★★★ 修正点 No.1: 偽共有対策用の構造体を定義 ★★★
// キューとMutexをグループ化し、キャッシュラインサイズ（例: 128バイト）に合わせる
struct PaddedQueue
{
    deque<TxnProto *> q;
    mutex m;
    // 構造体の合計サイズを調整するためのパディング
    char padding[128 - sizeof(deque<TxnProto *>) - sizeof(mutex)];
};

class DeterministicScheduler : public Scheduler
{
public:
    // (コンストラクタのシグネチャは変更なし)
    DeterministicScheduler(Configuration *conf,
                           Connection *rw_connection,
                           vector<Connection *> *ro_connections,
                           Storage *storage,
                           const Application *application);

    virtual ~DeterministicScheduler();

private:
    friend class MockDeterministicScheduler;
    static void *RunWorkerThread(void *arg);
    static void *LockManagerThread(void *arg);
    static void *RODispatcherThread(void *arg);

    void SendTxnPtr(socket_t *socket, TxnProto *txn);
    TxnProto *GetTxnPtr(socket_t *socket, zmq::message_t *msg);

    // --- メンバー変数の宣言 ---
    Configuration *configuration_;
    Connection *rw_connection_;
    vector<Connection *> *ro_connections_;
    Storage *storage_;
    const Application *application_;
    std::atomic<int> executing_txns_;

    // ★★★ 修正点 No.2: ワーカーキューの宣言を新しい構造体に変更 ★★★
    vector<PaddedQueue> worker_ro_queues_;

    DeterministicLockManager *lock_manager_;
    std::deque<TxnProto *> *ready_txns_;
    AtomicQueue<TxnProto *> *rw_txns_queue_;
    AtomicQueue<TxnProto *> *done_queue;
    AtomicQueue<MessageProto> *message_queues[NUM_WORKERS];

    // (スレッド関連、レイテンシ測定用変数は変更なし)
    pthread_t threads_[NUM_WORKERS];
    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_threads_[NUM_RO_DISPATCHERS];
    Connection *thread_connections_[NUM_WORKERS];

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