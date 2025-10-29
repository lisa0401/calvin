#ifndef DB_BACKEND_SIMPLE_STORAGE_H_
#define DB_BACKEND_SIMPLE_STORAGE_H_

#include <pthread.h>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <map>
#include <memory>
#include <mutex>
#include <utility>
#include <vector>

#include "backend/storage.h"  // Key, Value, int64, Storage の宣言

class SimpleStorage : public Storage {
 public:
  // ============ 型 ============

  // Key→Value* のイミュータブル表
  using Table = std::map<Key, Value *>;

  // 世代（エポック）ごとのスナップショット情報
  struct Snapshot {
    std::shared_ptr<const Table> tbl;   // 読み取り用テーブル（不変）
    std::atomic<int> readers;           // Pin/Unpin 用の参照カウント
    int64 cut_end;                      // このエポックがカバーする最大 cut（B*MAX-1）
    Snapshot(std::shared_ptr<const Table> t, int readers0, int64 c)
        : tbl(std::move(t)), readers(readers0), cut_end(c) {}
  };

  using EpochMap = std::map<int64, std::shared_ptr<Snapshot>>;

  // 差分（デルタ）用のシャード
  struct DeltaShard {
    pthread_mutex_t mu;
    Table map;
    DeltaShard() : mu(PTHREAD_MUTEX_INITIALIZER) {}
  };

  // スナップショット公開要求（LockManagerThread → SnapshotThread）
  struct SnapshotRequest {
    std::shared_ptr<const Table> base_table;  // 現在の stable_curr_（基準）
    int64 old_cut = -1;                       // 直前の cut_curr_
    int64 new_cut = -1;                       // 新しく公開する cut
    std::vector<Table> captured_deltas;       // シャードから吸い上げた差分
  };

  // ============ 定数 ============
  static constexpr size_t kDeltaShards = 64;

  // ============ 構築 / 破棄 ============
  SimpleStorage();
  ~SimpleStorage() override;

  // ============ Storage 抽象メソッド実装 ============
  // RW 読み取り（txn_id は cut として扱う。負値なら最新版）
  Value *ReadObject(const Key &key, int64 txn_id = 0) override;

  // 書込みは差分へ（RW）
  bool PutObject(const Key &key, Value *value, int64 txn_id = 0) override;
  bool DeleteObject(const Key &key, int64 txn_id = 0) override;

  // 事前取得（この実装では no-op）
  bool Prefetch(const Key &key, double *wait_time) override;
  bool Unfetch(const Key &key) override;

  // ============ 追加 API（RO fast-path 用） ============
  // 指定エポックのスナップショットから読み取り（RO）
  Value *ReadObjectAtEpoch(const Key &key, int64 epoch);

  // 現在公開済みの最新 cut / epoch を取得（RO が参照）
  int64 LatestPublishedCut() const;
  int64 LatestPublishedEpoch() const;

  // cut（txn_id 空間）に対応する epoch を返す（存在しないなら最も近い過去の epoch）
  int64 EpochForCut(int64 cut) const;

  // ピン/アンピン（GC 用）。RO fast-path では基本未使用だが互換のため残す。
  void PinEpoch(int64 epoch) override;
  void UnpinEpoch(int64 epoch) override;

  // エポック操作ユーティリティ
  bool CanRecycleUpTo(int64 epoch) const;
  void WaitUntilNoReaders(int64 epoch);
  int64 CurrentEpoch() const;

  // ============ スナップショット公開 ============
  // 高速部：差分を吸い上げて SnapshotRequest を構築（ロック最小化）
  SnapshotRequest *CaptureDeltasAndCreateRequest(int64 new_curr_cut);

  // 低速部：差分適用 → stable_{prev,curr}_ の入れ替え → RCU マップ更新 → GC → 公開通知
  void ApplyAndPublishSnapshot(SnapshotRequest *req);

 private:
  // Tombstone：削除マーカー
  static inline Value *Tombstone() {
    return reinterpret_cast<Value *>(-1);
  }

  // シャード関数
  static inline size_t ShardFor(const Key &key) {
    // 簡易ハッシュ（Key が std::string 相当なら std::hash で OK）
    return std::hash<Key>{}(key) % kDeltaShards;
  }

  // 参照カットの切り替え／世代 GC（hist_mu_ 保持中に呼ぶ）
  void GCUnlocked(EpochMap *map_to_gc);

 private:
  // ============ 差分（RW 書込み先） ============
  std::vector<DeltaShard> delta_;

  // ============ 安定スナップショット ============
  std::shared_ptr<const Table> stable_prev_;  // 1つ前に公開したテーブル
  std::shared_ptr<const Table> stable_curr_;  // 現在の安定テーブル

  // ============ cut （txn_id 空間）の境界 ============
  std::atomic<int64> cut_prev_;  // stable_prev_ がカバーする最大 cut
  std::atomic<int64> cut_curr_;  // stable_curr_ がカバーする最大 cut

  // ============ 公開済みメタ（RO参照用） ============
  std::atomic<int64> latest_published_cut_;
  std::atomic<int64> latest_published_epoch_;

  // ============ RCU 風のエポック・スナップショットマップ ============
  // 注意：std::shared_ptr は free 関数の atomic_load/atomic_store で RCU 的に切替
  std::shared_ptr<const EpochMap> epochs_ptr_;

  // ============ ヒストリ管理 ============
  mutable std::mutex hist_mu_;
  std::condition_variable hist_cv_;
  int64 curr_epoch_ = 0;
  int64 prev_epoch_ = -1;
  size_t max_history_epochs_ = 3;  // 古い世代を保持する最大数（調整可）

  // ============ 非コピー ============
  SimpleStorage(const SimpleStorage &) = delete;
  SimpleStorage &operator=(const SimpleStorage &) = delete;
};

#endif  // DB_BACKEND_SIMPLE_STORAGE_H_
