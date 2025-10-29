#ifndef _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
#define _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_

#include <pthread.h>
#include <atomic>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <vector>
#include <unordered_map>
#include <condition_variable>

#include "scheduler/scheduler.h"
#include "common/utils.h"         // AtomicQueue / GetTime など
#include "common/definitions.hh"  // NUM_WORKERS, NUM_RO_DISPATCHERS など
#include "proto/txn.pb.h"
#include "proto/message.pb.h"

// RO/RWの公開スナップショット作成リクエスト型を使うため
#include "backend/simple_storage.h"

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
 * - RO は「自分の batch_number - 1 の epoch（スナップショット）」を読む前提（実装側ではPinしない）。
 * - Publish（スナップショット適用）は、対象バッチ（= B-1）の RW・RO の双方が完了してから行う。
 */
class DeterministicScheduler : public Scheduler {
public:
    DeterministicScheduler(Configuration* conf,
                           Connection* rw_connection,
                           std::vector<Connection*>* ro_connections,
                           Storage* storage,
                           const Application* application);
    ~DeterministicScheduler() override;

    // ==== 公開状態 ====
    // RO が読む「最後にコミット済みのバッチ番号」
    std::atomic<uint64_t> last_committed_batch_{0};

    // Publish 済みの最新エポック番号（ApplyAndPublishSnapshot 後に更新）
    std::atomic<uint64_t> published_epoch_{0};

    // ==== バッチ単位のRW進行状況 ====
    // B バッチの残RW件数
    std::map<int, int> pending_rw_per_batch_;
    int                next_batch_to_commit_ = 0;
    std::mutex         pending_mu_;

    // ==== 静穏化（quiescence）判定 ====
    // 対象バッチの「スナップショット待機中RO件数」（ROはBバッチに到着 → B-1 を読むため waitはB-1）
    std::unordered_map<int, int> ro_inflight_per_batch_;
    std::mutex                   quiescence_mu_;
    std::condition_variable      quiescence_cv_;

    // 対象バッチ B が「RW=0 かつ RO=0」になるまで待機
    void WaitUntilBatchQuiescent(int B);

private:
    friend class MockDeterministicScheduler;

    // ===================== スレッドエントリ =====================
    static void* RunWorkerThread(void* arg);
    static void* LockManagerThread(void* arg);
    static void* RODispatcherThread(void* arg);
    static void* SnapshotThreadMain(void* arg); // スナップショット用スレッド

    // ===================== ZMQユーティリティ（使用時のみ） =====================
    void      SendTxnPtr(zmq::socket_t* socket, TxnProto* txn);
    TxnProto* GetTxnPtr(zmq::socket_t* socket, zmq::message_t* msg);

    // ===================== 構成・参照 =====================
    Configuration*        configuration_;
    Connection*           rw_connection_;
    std::vector<Connection*>* ro_connections_;
    Storage*              storage_;
    const Application*    application_;

    // 実行中トランザクション数（計測用）
    std::atomic<int> executing_txns_{0};

    // ===================== キュー / マネージャ =====================
    // RO用ロックフリー・キュー（ワーカー毎）
    AtomicQueue<TxnProto*>*    ro_queues_[NUM_WORKERS];

    // RWロック獲得済みの待ち行列（実行待ち）
    std::deque<TxnProto*>*     ready_txns_;

    // ロックマネージャ
    DeterministicLockManager*  lock_manager_;

    // 実行ワーカーへ渡すRWキュー
    AtomicQueue<TxnProto*>*    rw_txns_queue_;

    // 完了通知（RO/RW共通）
    AtomicQueue<TxnProto*>*    done_queue;

    // ワーカーごとの READ_RESULT 受信用
    AtomicQueue<MessageProto>* message_queues[NUM_WORKERS];

    // ワーカースレッドとSequencer間のチャンネル（Link/Unlink 用）
    Connection*                thread_connections_[NUM_WORKERS];

    // --- スナップショット要求（LockManagerThread → SnapshotThreadMain） ---
    AtomicQueue<SimpleStorage::SnapshotRequest*>* snapshot_queue_;

    // ===================== スレッドハンドル =====================
    pthread_t threads_[NUM_WORKERS];
    pthread_t lock_manager_thread_;
    pthread_t ro_dispatcher_threads_[NUM_RO_DISPATCHERS];
    pthread_t snapshot_thread_; // スナップショット適用・公開スレッド

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

    // Dispatcher 全体で共有するラウンドロビン用チケット（RO）
    static std::atomic<uint64_t> ro_rr_ticket_;
};

#endif  // _DB_SCHEDULER_DETERMINISTIC_SCHEDULER_H_
