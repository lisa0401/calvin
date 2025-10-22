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
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_init(&delta_[s].mu, nullptr);
  }

  // RCUのための初期マップを作成
  {
    std::lock_guard<std::mutex> lk(hist_mu_); 
    // ▼ 変更 ▼
    auto initial_map = std::make_shared<EpochMap>(); // 型を EpochMap に
    
    // Snapshot を make_shared で生成
    (*initial_map)[0] = std::make_shared<Snapshot>(
        std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire),
        /*readers=*/0,
        /*cut_end=*/-1
    );
    
    // アトミックポインタに初期マップを格納
    std::atomic_store_explicit(&atomic_epochs_ptr_, 
                               std::shared_ptr<const EpochMap>(initial_map), // 型を EpochMap に
                               std::memory_order_relaxed);
    curr_epoch_ = 0;
    // ▲ 変更 ▲
  }
  prev_epoch_ = -1;
}

SimpleStorage::~SimpleStorage() {
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_destroy(&delta_[s].mu);
  }
}

// --- 読取り（RW）---
Value* SimpleStorage::ReadObject(const Key& key, int64 txn_id) {
  // (この関数は変更なし)
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
  auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  auto it = sc->find(key);
  return (it == sc->end() || it->second == Tombstone()) ? nullptr : it->second;
}


// --- 読取り（RO）---
Value* SimpleStorage::ReadObjectAtEpoch(const Key& key, int64 epoch) {
  std::shared_ptr<const Table> snap;
  {
    auto epochs_map = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_acquire);

    auto it = epochs_map->find(epoch);
    if (it == epochs_map->end()) {
      snap = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
    } else {
      // ▼ 変更 ▼
      snap = it->second->tbl; // shared_ptr を介してアクセス
      // ▲ 変更 ▲
    }
  }
  auto it = snap->find(key);
  return (it == snap->end() || it->second == Tombstone()) ? nullptr : it->second;
}

// --- 書込み（RW）---
bool SimpleStorage::PutObject(const Key& key, Value* value, int64 /*txn_id*/) {
  // (変更なし)
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);
  delta_[s].map[key] = value;
  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}

bool SimpleStorage::DeleteObject(const Key& key, int64 /*txn_id*/) {
  // (変更なし)
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);
  delta_[s].map[key] = Tombstone();
  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}


// --- 世代参照管理（ROのPin/Unpin） ---
void SimpleStorage::PinEpoch(int64 epoch) {
  auto epochs_map = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_acquire);

  auto it = epochs_map->find(epoch);
  if (it != epochs_map->end()) {
    // ▼ 変更 ▼
    it->second->readers.fetch_add(1, std::memory_order_acq_rel); // shared_ptr を介してアクセス
    // ▲ 変更 ▲
  }
}

void SimpleStorage::UnpinEpoch(int64 epoch) {
  auto epochs_map = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_acquire);

  auto it = epochs_map->find(epoch);
  if (it != epochs_map->end()) {
    // ▼ 変更 ▼
    it->second->readers.fetch_sub(1, std::memory_order_acq_rel); // shared_ptr を介してアクセス
    // ▲ 変更 ▲
  }
}

bool SimpleStorage::CanRecycleUpTo(int64 epoch) const {
  auto epochs_map = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_acquire);

  for (auto it = epochs_map->begin(); it != epochs_map->end() && it->first <= epoch; ++it) {
    // ▼ 変更 ▼
    if (it->second->readers.load(std::memory_order_acquire) != 0) return false;
    // ▲ 変更 ▲
  }
  return true;
}

void SimpleStorage::WaitUntilNoReaders(int64 epoch) {
  std::unique_lock<std::mutex> lk(hist_mu_); 
  hist_cv_.wait(lk, [&] {
    auto epochs_map = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_acquire);

    auto it = epochs_map->upper_bound(epoch);
    for (auto jt = epochs_map->begin(); jt != it; ++jt) {
      // ▼ 変更 ▼
      if (jt->second->readers.load(std::memory_order_acquire) != 0) return false;
      // ▲ 変更 ▲
    }
    return true;
  });
}

// --- Storage基底クラスの純粋仮想関数の実装 ---
bool SimpleStorage::Prefetch(const Key& key, double* wait_time) {
    if (wait_time) *wait_time = 0.0;
    return true; 
}
bool SimpleStorage::Unfetch(const Key& key) {
    return true; 
}


// --- スナップショット公開 [高速] ---
SimpleStorage::SnapshotRequest* SimpleStorage::CaptureDeltasAndCreateRequest(int64 new_curr_cut) {
  // (変更なし)
  auto req = new SnapshotRequest();
  req->base_table = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  req->old_cut = cut_curr_.load(std::memory_order_acquire);
  req->new_cut = new_curr_cut;
  assert(new_curr_cut >= req->old_cut);
  for (size_t s = 0; s < kDeltaShards; ++s) {
      pthread_mutex_lock(&delta_[s].mu);
      if (!delta_[s].map.empty()) {
          Table tmp;
          tmp.swap(delta_[s].map);
          req->captured_deltas.emplace_back(std::move(tmp));
      }
      pthread_mutex_unlock(&delta_[s].mu);
  }
  return req;
}

// --- スナップショット公開 [低速] ---
void SimpleStorage::ApplyAndPublishSnapshot(SnapshotRequest* req) {
    auto base_table = req->base_table;
    bool any_delta = !req->captured_deltas.empty();

    std::lock_guard<std::mutex> lk(hist_mu_);

    auto old_map_ptr = std::atomic_load_explicit(&atomic_epochs_ptr_, std::memory_order_relaxed);
    
    // ▼ 変更 ▼
    // [重い処理 1] マップを丸ごとコピーする (型を EpochMap に)
    auto new_map_ptr = std::make_shared<EpochMap>(*old_map_ptr);
    // ▲ 変更 ▲

    std::shared_ptr<const Table> next_table;

    if (!any_delta) {
        // 変更なし
        next_table = base_table;
        std::atomic_store_explicit(&stable_prev_, base_table, std::memory_order_release);
        cut_prev_.store(req->old_cut, std::memory_order_release);
        cut_curr_.store(req->new_cut, std::memory_order_release);
    } else {
        // 変更あり
        // [重い処理 2] curr をクローンして delta を適用
        auto next = std::make_shared<Table>(*base_table);
        for (const auto& shard_map : req->captured_deltas) {
            for (const auto& kv : shard_map) {
                if (kv.second == Tombstone())
                    next->erase(kv.first);
                else
                    (*next)[kv.first] = kv.second;
            }
        }
        next_table = std::shared_ptr<const Table>(next);

        // 公開
        std::atomic_store_explicit(&stable_prev_, base_table, std::memory_order_release);
        std::atomic_store_explicit(&stable_curr_, next_table, std::memory_order_release);
        cut_prev_.store(req->old_cut, std::memory_order_release);
        cut_curr_.store(req->new_cut, std::memory_order_release);
    }

    // 履歴登録 & epoch を前進して GC
    const int64 new_epoch = curr_epoch_ + 1;
    
    // ▼ 変更 ▼
    // Snapshot を make_shared で生成
    (*new_map_ptr)[curr_epoch_] = std::make_shared<Snapshot>(base_table, 0, req->old_cut);
    (*new_map_ptr)[new_epoch] = std::make_shared<Snapshot>(next_table, 0, req->new_cut);
    // ▲ 変更 ▲

    prev_epoch_ = curr_epoch_;
    curr_epoch_ = new_epoch;
    GCUnlocked(new_map_ptr.get());

    // (Update) 新しいマップをアトミックに公開
    // ▼ 変更 ▼
    std::atomic_store_explicit(&atomic_epochs_ptr_, 
                               std::shared_ptr<const EpochMap>(new_map_ptr), // 型を EpochMap に
                               std::memory_order_release);
    // ▲ 変更 ▲
    
    delete req;
}


// --- 読者0の古い世代を掃く（hist_mu_ 保持中に呼ぶ） ---
// ▼ 変更 ▼
void SimpleStorage::GCUnlocked(EpochMap* map_to_gc) { // 引数の型を変更
// ▲ 変更 ▲
  if (max_history_epochs_ == 0) return; 
  
  while (map_to_gc->size() > max_history_epochs_) {
    auto it = map_to_gc->begin();
    if (it->first >= prev_epoch_) break; 
    
    // ▼ 変更 ▼
    if (it->second->readers.load(std::memory_order_acquire) == 0) { // shared_ptr を介してアクセス
    // ▲ 変更 ▲
      map_to_gc->erase(it);
    } else {
      break;
    }
  }
}