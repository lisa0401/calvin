#pragma once
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <vector>
#include <pthread.h>

#include "backend/storage.h"  // 基底IFに PinEpoch/UnpinEpoch/ReadObjectAtEpoch 等を no-op で追加済み前提

class SimpleStorage : public Storage {
public:
    using Key   = std::string;
    using Value = std::string;                  // 既存定義に合わせてください
    using Table = std::unordered_map<Key, Value*>;

    SimpleStorage();
    ~SimpleStorage();

    // ---- Storage IF ----
    Value* ReadObject(const Key& key, int64 txn_id) override;
    bool   PutObject(const Key& key, Value* value, int64 /*txn_id*/) override;
    bool   DeleteObject(const Key& key, int64 /*txn_id*/) override;

    // メモリ内ストレージなのでプリフェッチは実質 no-op
    bool Prefetch(const Key& /*key*/, double* wait_time) override {
        if (wait_time) *wait_time = 0.0;
        return true;
    }
    bool Unfetch(const Key& /*key*/) override { return true; }

    void Initmutex() override {} // 互換用（未使用）
    void PublishSnapshot(int64 new_curr_cut) override;

    // === multi-epoch: RO向け API ===
    Value* ReadObjectAtEpoch(const Key& key, int64 epoch) override;
    void   PinEpoch(int64 epoch) override;
    void   UnpinEpoch(int64 epoch) override;
    bool   CanRecycleUpTo(int64 epoch) const override;
    void   WaitUntilNoReaders(int64 epoch) override;

private:
    // Tombstone: 予約アドレスを番兵値として使用（Storage::Tombstone() に依存しない）
    static inline Value* Tombstone() {
        return reinterpret_cast<Value*>(uintptr_t(0x1));
    }

    // ---- シャーディングされた delta（エポック内の差分）----
    struct DeltaShard {
        pthread_mutex_t mu{};
        Table map;
        DeltaShard() { pthread_mutex_init(&mu, nullptr); }
        ~DeltaShard(){ pthread_mutex_destroy(&mu); }
        DeltaShard(const DeltaShard&) = delete;
        DeltaShard& operator=(const DeltaShard&) = delete;
        DeltaShard(DeltaShard&&) = default;
        DeltaShard& operator=(DeltaShard&&) = default;
    };

    static constexpr size_t kDeltaShards = 256; // 2の冪：マスクが使える
    static inline size_t ShardFor(const Key& k) {
        return std::hash<Key>{}(k) & (kDeltaShards - 1); // 高速モジュロ
    }
    std::vector<DeltaShard> delta_; // in-place 更新（コピー/CAS なし）

    // ---- RCU 高速パス（ROは prev/curr をロックレス参照）----
    // ※ atomic<shared_ptr> は使わず、.cc で free 関数の atomic_load/store を使用する
    std::shared_ptr<const Table> stable_prev_{std::make_shared<Table>()};
    std::shared_ptr<const Table> stable_curr_{std::make_shared<Table>()};

    // スナップショット境界（txn_id の上限）
    std::atomic<int64> cut_prev_{-1};
    std::atomic<int64> cut_curr_{-1};

    // ---- 履歴世代（multi-epoch）＋参照カウント ----
        struct Snapshot {
        std::shared_ptr<const Table> tbl; // 当該 epoch の不変テーブル
        std::atomic<int> readers{0};      // 読んでいるRO数
        int64 cut_end{-1};                // その epoch の最大 txn_id

        Snapshot() = default;

        Snapshot(std::shared_ptr<const Table> t, int r, int64 c)
            : tbl(std::move(t)), readers(r), cut_end(c) {}

        // ✅ ムーブコンストラクタ
        Snapshot(Snapshot&& other) noexcept
            : tbl(std::move(other.tbl)),
              readers(other.readers.load(std::memory_order_relaxed)),
              cut_end(other.cut_end) {}

        // ✅ ムーブ代入演算子
        Snapshot& operator=(Snapshot&& other) noexcept {
            if (this != &other) {
                tbl = std::move(other.tbl);
                readers.store(other.readers.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
                cut_end = other.cut_end;
            }
            return *this;
        }

        // コピーは禁止（安全のため）
        Snapshot(const Snapshot&) = delete;
        Snapshot& operator=(const Snapshot&) = delete;
    };


    mutable std::mutex              hist_mu_;
    std::condition_variable         hist_cv_;
    std::map<int64, Snapshot>       epochs_;      // epoch -> Snapshot
    int64                           curr_epoch_{0};
    int64                           prev_epoch_{-1};

    // メモリ安全弁（0なら無制限）：保持する最大 epoch 数（古い順にGC）
    size_t                          max_history_epochs_{4};

    // 読者0の古い世代を掃除（hist_mu_ ロック下で呼ぶ）
    void GCUnlocked();
};
