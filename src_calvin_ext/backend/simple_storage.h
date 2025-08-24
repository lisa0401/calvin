#pragma once
#include <atomic>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>
#include <pthread.h>
#include "backend/storage.h" // 既存IF

class SimpleStorage : public Storage
{
public:
    using Key = std::string;
    using Value = std::string; // 既存定義に合わせてください
    using Table = std::unordered_map<Key, Value *>;

    SimpleStorage();

    // ---- Storage IF ----
    Value *ReadObject(const Key &key, int64 txn_id) override;
    bool PutObject(const Key &key, Value *value, int64 /*txn_id*/) override;
    bool DeleteObject(const Key &key, int64 /*txn_id*/) override;

    // メモリ内ストレージなのでプリフェッチは実質 no-op
    bool Prefetch(const Key & /*key*/, double *wait_time) override
    {
        if (wait_time)
            *wait_time = 0.0;
        return true;
    }
    bool Unfetch(const Key & /*key*/) override { return true; }

    void Initmutex() override {} // 互換用（未使用）
    void PublishSnapshot(int64 new_curr_cut) override;

private:
    // Tombstone: 予約アドレスを番兵値として使用（Storage::Tombstone() に依存しない）
    static inline Value *Tombstone()
    {
        return reinterpret_cast<Value *>(uintptr_t(0x1));
    }

    // ---- シャーディングされた delta（エポック内の差分）----
    struct DeltaShard
    {
        pthread_mutex_t mu;
        Table map;
        DeltaShard() { pthread_mutex_init(&mu, nullptr); }
        ~DeltaShard() { pthread_mutex_destroy(&mu); }
        DeltaShard(const DeltaShard &) = delete;
        DeltaShard &operator=(const DeltaShard &) = delete;
        DeltaShard(DeltaShard &&) = default;
        DeltaShard &operator=(DeltaShard &&) = default;
    };

    static constexpr size_t kDeltaShards = 256; // 2の冪：マスクが使える
    static inline size_t ShardFor(const Key &k)
    {
        // 高速なモジュロ用にビットマスク
        return std::hash<Key>{}(k) & (kDeltaShards - 1);
    }
    std::vector<DeltaShard> delta_; // in-place で更新（コピーもCASループも無し）

    // ---- 2世代RCU（ROは prev/curr をロックレス参照）----
    std::shared_ptr<const Table> stable_prev_; // 前スナップショット
    std::shared_ptr<const Table> stable_curr_; // 現行スナップショット

    // スナップショット境界（txn_id の上限）
    std::atomic<int64> cut_prev_{-1};
    std::atomic<int64> cut_curr_{-1};
};
