#ifndef _BACKEND_SIMPLE_STORAGE_H_
#define _BACKEND_SIMPLE_STORAGE_H_

#include "backend/storage.h"

#include <pthread.h>
#include <atomic>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

class SimpleStorage : public Storage {
public:
    using Table = std::map<Key, Value*>;

    // ROトランザクション用のスナップショット（世代）定義
    struct Snapshot {
        std::shared_ptr<const Table> tbl; // スナップショット本体
        mutable std::atomic<uint32_t> readers; // 参照中のROトランザクション数
        int64 cut_end; // このスナップショットが有効な最終TXN ID
        
        // ▼ 追加 ▼
        // コンストラクタ (atomic は { } で初期化できないため)
        Snapshot(std::shared_ptr<const Table> t, uint32_t r, int64 c)
            : tbl(t), readers(r), cut_end(c) {}
    };

    // ▼ 変更 ▼
    // マップの「値」を Snapshot そのものではなく、Snapshot へのポインタに変更
    using EpochMap = std::map<int64, std::shared_ptr<Snapshot>>;

    // スナップショット作成"依頼"を定義する構造体
    struct SnapshotRequest {
        std::shared_ptr<const Table> base_table;
        std::vector<Table> captured_deltas;
        int64 old_cut;
        int64 new_cut;
    };

    SimpleStorage();
    virtual ~SimpleStorage();
    int64 CurrentEpoch() const;

    // --- トランザクション実行 (Worker) / ロック管理 (LM) からの I/O ---
    virtual Value* ReadObject(const Key& key, int64 txn_id) override;
    virtual bool PutObject(const Key& key, Value* value, int64 txn_id) override;
    virtual bool DeleteObject(const Key& key, int64 txn_id) override;

    // --- ROトランザクション (RODispatcher) 用の I/O ---
    virtual Value* ReadObjectAtEpoch(const Key& key, int64 epoch);
    virtual void PinEpoch(int64 epoch) override;
    virtual void UnpinEpoch(int64 epoch) override;

    // --- Storage 基底クラスの純粋仮想関数 (空実装) ---
    virtual bool Prefetch(const Key& key, double* wait_time) override;
    virtual bool Unfetch(const Key& key) override;

    // --- スナップショット管理 (LockManager / SnapshotThread) ---
    virtual SnapshotRequest* CaptureDeltasAndCreateRequest(int64 new_curr_cut);
    virtual void ApplyAndPublishSnapshot(SnapshotRequest* req);

    // --- GC / デバッグ用 ---
    virtual bool CanRecycleUpTo(int64 epoch) const;
    virtual void WaitUntilNoReaders(int64 epoch);


private:
    static Value* Tombstone() {
        return reinterpret_cast<Value*>(0xDEADBEEF);
    }
    static size_t ShardFor(const Key& key) {
        return std::hash<Key>()(key) % kDeltaShards;
    }

    // ▼ 変更 ▼
    void GCUnlocked(EpochMap* map_to_gc); // 引数の型を変更

    static const size_t kDeltaShards = 64;
    static const size_t max_history_epochs_ = 64;

    struct Shard {
        alignas(64) pthread_mutex_t mu;
        Table map;
    };

    std::vector<Shard> delta_;
    std::shared_ptr<const Table> stable_prev_;
    std::shared_ptr<const Table> stable_curr_;
    std::atomic<int64> cut_prev_;
    std::atomic<int64> cut_curr_;

    // --- RO用 RCU (Read-Copy-Update) 世代管理 ---

    // ▼ 変更 ▼
    // マップのポインタを保持 (マップの型が EpochMap に)
    std::shared_ptr<const EpochMap> atomic_epochs_ptr_;

    std::mutex hist_mu_;
    std::condition_variable hist_cv_; 
    int64 curr_epoch_;
    int64 prev_epoch_;
};

#endif // _BACKEND_SIMPLE_STORAGE_H_