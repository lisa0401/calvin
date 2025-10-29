#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <vector>

#include "scheduler/scheduler.h"
#include "common/utils.h"         // AtomicQueue / GetTime など
#include "common/definitions.hh"  // NUM_WORKERS, NUM_RO_DISPATCHERS など
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

// ▼ 追加 ▼
// SimpleStorage::SnapshotRequest* を使うためにインクルード
#include "backend/simple_storage.h"
// ▲ 追加 ▲

// （必要なら）ZeroMQ 前方宣言
namespace zmq {
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

/**
 * DeterministicScheduler
 * - Sequencer から届く RW/RO Txn を受け取り、ロック取得・実行キュー投入・完了回収を担当。
 * - RO は「自分の batch_number - 1 の epoch（スナップショット）」を読む前提。
 * - multi-epoch Storage（PinEpoch/UnpinEpoch）と連携して、遅延ROがいても publish を止めずに前に進める。
 */
class DeterministicScheduler : public Scheduler {
public:
    DeterministicScheduler(Configuration* conf,
                           Connection* rw_connection,
                           std::vector<Connection*>* ro_connections,
                           Storage* storage,
                           const Application* application);
    ~DeterministicScheduler() override;

    // RO が読む「最後にコミット済みのバッチ番号」
    std::atomic<uint64_t> last_committed_batch_{0};

    // ★ 追加：公開（publish）済みの最新エポック番号
    // SnapshotThreadMain で ApplyAndPublishSnapshot 完了直後に更新し、
    // RODispatcherThread からスナップショット epoch のクリップに使用する。
    std::atomic<uint64_t> published_epoch_{0};

    // RW コミット前缶詰数（バッチ単位）
    std::map<int, int> pending_rw_per_batch_;
    int next_batch_to_commit_ = 0;
    std::mutex pending_mu_;

private:
    friend class MockDeterministicScheduler;

    // ===================== スレッドエントリ =====================
    static void* RunWorkerThread(void* arg);
    static void* LockManagerThread(void* arg);
    static void* RODispatcherThread(void* arg);
    // ▼ 追加 ▼
    static void* SnapshotThreadMain(void* arg); // スナップショット用スレッド
    // ▲ 追加 ▲

    // ===================== ZMQユーティリティ（使うなら） =====================
    // ZMQ 経由で TxnProto* を送受信（使用しない構成なら未使用でOK）
    void      SendTxnPtr(zmq::socket_t* socket, TxnProto* txn);
    TxnProto* GetTxnPtr(zmq::socket_t* socket, zmq::message_t* msg);

    // ===================== 構成 =====================
    Configuration* configuration_;
    Connection* rw_connection_;
    std::vector<Connection*>* ro_connections_;
    Storage* storage_;
    const Application* application_;

    // 実行中トランザクション数（計測用）— 複数スレッドから更新されるため atomic
    std::atomic<int> executing_txns_{0};

    // ===================== キュー / マネージャ =====================
    // ★ RO 用ロックフリー・キュー（ワーカ毎）: AtomicQueue は common/utils.h の実装を使用
    AtomicQueue<TxnProto*>* ro_queues_[NUM_WORKERS];

    // RWキュー / 完了キュー
    std::deque<TxnProto*>*      ready_txns_;     // ロック獲得済みで実行待ちのRW
    DeterministicLockManager*   lock_manager_;
    AtomicQueue<TxnProto*>*     rw_txns_queue_;  // 実行ワーカーへ渡すRW
    AtomicQueue<TxnProto*>*     done_queue;      // 完了通知（RO/RW共通）
    AtomicQueue<MessageProto>*  message_queues[NUM_WORKERS];
    Connection*                 thread_connections_[NUM_WORKERS];

    // ▼ 追加 ▼
    // スナップショット要求を格納するキュー（LockManagerThread → SnapshotThreadMain）
    AtomicQueue<SimpleStorage::SnapshotRequest*>* snapshot_queue_;
    // ▲ 追加 ▲

    // ===================== スレッドハンドル =====================
    pthread_t threads_[NUM_WORKERS];
    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_threads_[NUM_RO_DISPATCHERS];
    // ▼ 追加 ▼
    pthread_t snapshot_thread_; // スナップショット用スレッド
    // ▲ 追加 ▲

    // ===================== 計測（RW） =====================
    std::atomic<double> total_sequencer_time_{0};
    std::atomic<double> total_queueing_time_{0};
    std::atomic<double> total_worker_time_{0};
    std::atomic<int>    processed_rwt_count_{0};

    // ===================== 計測（RO） =====================
    std::atomic<double> total_ro_dispatch_time_{0};
    std::atomic<double> total_ro_queueing_time_{0};
    std::atomic<double> total_ro_worker_time_{0};
    std::atomic<int>    processed_rot_count_{0};

    // ★ Dispatcher 全体で共有するラウンドロビン用チケット
    static std::atomic<uint64_t> ro_rr_ticket_;
};

#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
