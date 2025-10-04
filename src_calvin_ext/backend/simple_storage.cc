#include "backend/simple_storage.h"

#include <cassert>
#include <utility>
#include <vector>
#include <memory>
#include <atomic>
#include <map>
#include <mutex>
#include <condition_variable>

SimpleStorage::SimpleStorage()
    : delta_(kDeltaShards),
      stable_prev_(std::make_shared<Table>()),
      stable_curr_(std::make_shared<Table>()),
      cut_prev_(-1),
      cut_curr_(-1) {
  // delta シャードの mutex 初期化（未初期化なら）
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_init(&delta_[s].mu, nullptr);
  }

  // 初期 curr を epoch=0 として履歴に登録
  {
    std::lock_guard<std::mutex> lk(hist_mu_);
    epochs_[0] = Snapshot{
        std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire),
        /*readers=*/0,
        /*cut_end=*/-1};
    curr_epoch_ = 0;
  }
  prev_epoch_ = -1;
}

SimpleStorage::~SimpleStorage() {
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_destroy(&delta_[s].mu);
  }
}

// --- 読取り（ROは完全ロックレス）：
// txn_id <= prev_cut → prev を参照
// txn_id <= curr_cut → curr を参照
// それ以外（最新系＝RWなど）→ delta(シャードを軽ロック) → curr
Value* SimpleStorage::ReadObject(const Key& key, int64 txn_id) {
  const int64 prev_cut = cut_prev_.load(std::memory_order_acquire);
  const int64 curr_cut = cut_curr_.load(std::memory_order_acquire);

  if (txn_id >= 0 && txn_id <= prev_cut) {
    auto sp = std::atomic_load_explicit(&stable_prev_, std::memory_order_acquire);
    auto it = sp->find(key);
    return (it == sp->end() || it->second == Tombstone()) ? nullptr : it->second;
  }

  if (txn_id >= 0 && txn_id <= curr_cut) {
    auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
    auto it = sc->find(key);
    return (it == sc->end() || it->second == Tombstone()) ? nullptr : it->second;
  }

  // 最新系：まず delta を見る（該当シャードのみロック）
  {
    const size_t s = ShardFor(key);
    pthread_mutex_lock(&delta_[s].mu);
    auto it = delta_[s].map.find(key);
    if (it != delta_[s].map.end()) {
      Value* v = it->second;
      pthread_mutex_unlock(&delta_[s].mu);
      return (v == Tombstone()) ? nullptr : v;
    }
    pthread_mutex_unlock(&delta_[s].mu);
  }

  // 見つからなければ現行スナップショット
  auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  auto it = sc->find(key);
  return (it == sc->end() || it->second == Tombstone()) ? nullptr : it->second;
}

// --- RO向け：epoch を狙い撃ちで読む ---
Value* SimpleStorage::ReadObjectAtEpoch(const Key& key, int64 epoch) {
  std::shared_ptr<const Table> snap;
  {
    std::lock_guard<std::mutex> lk(hist_mu_);
    auto it = epochs_.find(epoch);
    if (it == epochs_.end()) {
      // 想定外：現行へフォールバック（運用次第で assert でもOK）
      snap = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
    } else {
      snap = it->second.tbl;
    }
  }
  auto it = snap->find(key);
  return (it == snap->end() || it->second == Tombstone()) ? nullptr : it->second;
}

// --- 書込み：シャード単位で in-place 更新（コピーもCASループも無し）---
bool SimpleStorage::PutObject(const Key& key, Value* value, int64 /*txn_id*/) {
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);
  delta_[s].map[key] = value;
  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 /*txn_id*/) {
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);
  delta_[s].map[key] = Tombstone();
  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}

// --- 世代参照管理（ROのPin/Unpin） ---
void SimpleStorage::PinEpoch(int64 epoch) {
  std::lock_guard<std::mutex> lk(hist_mu_);
  auto it = epochs_.find(epoch);
  if (it != epochs_.end()) {
    it->second.readers.fetch_add(1, std::memory_order_acq_rel);
  }
}

void SimpleStorage::UnpinEpoch(int64 epoch) {
  std::unique_lock<std::mutex> lk(hist_mu_);
  auto it = epochs_.find(epoch);
  if (it != epochs_.end()) {
    if (it->second.readers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      hist_cv_.notify_all();  // 0 になった
    }
  }
}

bool SimpleStorage::CanRecycleUpTo(int64 epoch) const {
  std::lock_guard<std::mutex> lk(hist_mu_);
  for (auto it = epochs_.begin(); it != epochs_.end() && it->first <= epoch; ++it) {
    if (it->second.readers.load(std::memory_order_acquire) != 0) return false;
  }
  return true;
}

void SimpleStorage::WaitUntilNoReaders(int64 epoch) {
  std::unique_lock<std::mutex> lk(hist_mu_);
  hist_cv_.wait(lk, [&] {
    auto it = epochs_.upper_bound(epoch);
    for (auto jt = epochs_.begin(); jt != it; ++jt) {
      if (jt->second.readers.load(std::memory_order_acquire) != 0) return false;
    }
    return true;
  });
}

// --- スナップショット公開：各シャードを swap で奪取して curr に一括マージ ---
void SimpleStorage::PublishSnapshot(int64 new_curr_cut) {
  // いま公開中の curr（ROが参照中のもの）
  auto curr = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  const int64 old_curr_cut = cut_curr_.load(std::memory_order_acquire);
  assert(new_curr_cut >= old_curr_cut);

  // delta を奪取（各シャードの map を O(1) swap）
  bool any_delta = false;
  std::vector<Table> captured;
  captured.reserve(kDeltaShards);
  for (size_t s = 0; s < kDeltaShards; ++s) {
    Table tmp;
    pthread_mutex_lock(&delta_[s].mu);
    if (!delta_[s].map.empty()) {
      any_delta = true;
      tmp.swap(delta_[s].map);
    }
    pthread_mutex_unlock(&delta_[s].mu);
    if (!tmp.empty()) captured.emplace_back(std::move(tmp));
  }

  if (!any_delta) {
    // 変更なしでも Publish は止めずに epoch を進める
    std::atomic_store_explicit(&stable_prev_, curr, std::memory_order_release);
    cut_prev_.store(old_curr_cut, std::memory_order_release);
    cut_curr_.store(new_curr_cut, std::memory_order_release);

    // 履歴に登録して epoch 前進
    {
      std::lock_guard<std::mutex> lk(hist_mu_);
      const int64 new_epoch = curr_epoch_ + 1;

      // old(curr_epoch_) を prev として残す（cut_end=old_curr_cut）
      epochs_[curr_epoch_] = Snapshot{curr, /*readers=*/0, old_curr_cut};

      // new(curr)（内容は同じでも epoch は進む）
      epochs_[new_epoch] = Snapshot{
          std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire),
          /*readers=*/0, new_curr_cut};

      prev_epoch_ = curr_epoch_;
      curr_epoch_ = new_epoch;

      GCUnlocked();  // 読者ゼロの古い世代を掃除（上限も適用）
    }
    return;
  }

  // 変更あり：curr をクローンして delta を適用 → next
  auto next = std::make_shared<Table>(*curr);
  for (const auto& shard_map : captured) {
    for (const auto& kv : shard_map) {
      if (kv.second == Tombstone())
        next->erase(kv.first);
      else
        (*next)[kv.first] = kv.second;
    }
  }

  // 公開：prev <- curr, curr <- next（ROはロックレスに到達できる）
  std::atomic_store_explicit(&stable_prev_, curr, std::memory_order_release);
  std::atomic_store_explicit(&stable_curr_, std::shared_ptr<const Table>(next),
                             std::memory_order_release);

  // cut を最後に進める（順序保証）
  cut_prev_.store(old_curr_cut, std::memory_order_release);
  cut_curr_.store(new_curr_cut, std::memory_order_release);

  // 履歴登録 & epoch を前進して GC
  {
    std::lock_guard<std::mutex> lk(hist_mu_);
    const int64 new_epoch = curr_epoch_ + 1;

    // old(curr) を prev として残す（cut_end=old_curr_cut）
    epochs_[curr_epoch_] = Snapshot{curr, /*readers=*/0, old_curr_cut};

    // 新しい curr を new_epoch として登録（cut_end=new_curr_cut）
    epochs_[new_epoch] = Snapshot{
        std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire),
        /*readers=*/0, new_curr_cut};

    prev_epoch_ = curr_epoch_;
    curr_epoch_ = new_epoch;

    GCUnlocked();
  }
}

// --- 読者0の古い世代を掃く（hist_mu_ 保持中に呼ぶ） ---
void SimpleStorage::GCUnlocked() {
  if (max_history_epochs_ == 0) return;  // 無制限
  // 古い順に、読者ゼロのものを削る。prev_epoch_ までは基本残す
  while (epochs_.size() > max_history_epochs_) {
    auto it = epochs_.begin();
    if (it->first >= prev_epoch_) break;  // 直前は残す
    if (it->second.readers.load(std::memory_order_acquire) == 0) {
      epochs_.erase(it);
    } else {
      // 読者がいるのでこれ以上削らない（次回以降に回す）
      break;
    }
  }
}
