#include "backend/simple_storage.h"

#include <cassert>
#include <utility>

#include <atomic>   // free 関数 atomic_load/atomic_store 用
#include <map>
#include <memory>
#include <mutex>
#include <vector>

// [★ 修正]
// Value* のライフサイクルを Table と連動させるための準備
namespace {
// 1. Tombstone の定義は .h の private (Value*)-1 を使うため、ここでは不要

// 2. Table が破棄される際に、内部の Value* も全て delete するカスタムデリータ
struct TableDeleter {
  void operator()(SimpleStorage::Table *t) const {
    if (t == nullptr) return;
    for (auto &kv : *t) {
      // [★ 修正] private な SimpleStorage::Tombstone() の代わりに、
      // ヘッダ定義と同じハードコード値 (-1) を使用する
      if (kv.second != reinterpret_cast<Value *>(-1)) {
        delete kv.second; // Tombstone 以外のポインタを解放
      }
    }
    delete t; // Table 本体を解放
  }
};
// 3. カスタムデリータ付きの TablePtr 型エイリアス
using TablePtr = std::shared_ptr<SimpleStorage::Table>;
}  // namespace


// ========== 構築/破棄 ==========

SimpleStorage::SimpleStorage()
    : delta_(kDeltaShards),
      // [★ 修正] new Table() に TableDeleter を指定して shared_ptr を作成
      stable_prev_(std::shared_ptr<Table>(new Table(), TableDeleter())),
      stable_curr_(std::shared_ptr<Table>(new Table(), TableDeleter())),
      cut_prev_(-1),
      cut_curr_(-1),
      latest_published_cut_(-1),
      latest_published_epoch_(0) {
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_init(&delta_[s].mu, nullptr);
  }

  // 初期エポックを登録（EpochMap = { 0: stable_curr_ }）
  {
    std::lock_guard<std::mutex> lk(hist_mu_);
    auto initial_map = std::make_shared<EpochMap>();
    (*initial_map)[0] = std::make_shared<Snapshot>(
        std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire),
        /*readers=*/0,
        /*cut_end=*/-1);
    std::atomic_store_explicit(
        &epochs_ptr_,
        std::shared_ptr<const EpochMap>(initial_map),
        std::memory_order_relaxed);
    curr_epoch_ = 0;
  }
  prev_epoch_ = -1;
}

SimpleStorage::~SimpleStorage() {
  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_destroy(&delta_[s].mu);
  }
  // [★ 修正] shared_ptr が TableDeleter を自動で呼び出しクリーンアップする
}

// ========== 読み取り（RW 経路） ==========
// (変更なし)
Value *SimpleStorage::ReadObject(const Key &key, int64 txn_id) {
  const int64 prev_cut = cut_prev_.load(std::memory_order_acquire);
  const int64 curr_cut = cut_curr_.load(std::memory_order_acquire);

  if (txn_id >= 0 && txn_id <= prev_cut) {
    auto sp = std::atomic_load_explicit(&stable_prev_, std::memory_order_acquire);
    auto it = sp->find(key);
    return (it == sp->end() || it->second == reinterpret_cast<Value *>(-1)) ? nullptr : it->second;
  }
  if (txn_id >= 0 && txn_id <= curr_cut) {
    auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
    auto it = sc->find(key);
    return (it == sc->end() || it->second == reinterpret_cast<Value *>(-1)) ? nullptr : it->second;
  }
  {
    // 最新（未公開）差分をまず探す
    const size_t s = ShardFor(key);
    pthread_mutex_lock(&delta_[s].mu);
    auto it = delta_[s].map.find(key);
    if (it != delta_[s].map.end()) {
      Value *v = it->second;
      pthread_mutex_unlock(&delta_[s].mu);
      return (v == reinterpret_cast<Value *>(-1)) ? nullptr : v;
    }
    pthread_mutex_unlock(&delta_[s].mu);
  }
  // 見つからなければ stable_curr_
  auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  auto it = sc->find(key);
  return (it == sc->end() || it->second == reinterpret_cast<Value *>(-1)) ? nullptr : it->second;
}

// ========== 読み取り（RO 経路：エポック指定） ==========
// (変更なし)
Value *SimpleStorage::ReadObjectAtEpoch(const Key &key, int64 epoch) {
  std::shared_ptr<const Table> snap;
  {
    auto epochs_map =
        std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
    // epoch 以下で最大のスナップショットを選択
    auto it = epochs_map->upper_bound(epoch);
    if (it == epochs_map->begin()) {
      // 何も公開されていないなら、現在の stable_curr_ を読む
      snap = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
    } else {
      --it;
      snap = it->second->tbl;
    }
  }
  auto it2 = snap->find(key);
  return (it2 == snap->end() || it2->second == reinterpret_cast<Value *>(-1)) ? nullptr : it2->second;
}

// ========== 書込み（RW 経路：差分へ） ==========
bool SimpleStorage::PutObject(const Key &key, Value *value, int64 /*txn_id*/) {
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);
  
  // [★ 修正] UAFバグ修正：
  // 1. 以前の Value* があれば delete する
  auto it = delta_[s].map.find(key);
  if (it != delta_[s].map.end() && it->second != reinterpret_cast<Value *>(-1)) {
    delete it->second;
  }
  // 2. Txn の Value を *ディープコピー* して Storage が所有権を持つ
  delta_[s].map[key] = new Value(*value); 

  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}

bool SimpleStorage::DeleteObject(const Key &key, int64 /*txn_id*/) {
  const size_t s = ShardFor(key);
  pthread_mutex_lock(&delta_[s].mu);

  // [★ 修正] UAFバグ修正：
  // 1. 以前の Value* があれば delete する
  auto it = delta_[s].map.find(key);
  if (it != delta_[s].map.end() && it->second != reinterpret_cast<Value *>(-1)) {
    delete it->second;
  }
  // 2. Tombstone (ポインタ) を設定
  delta_[s].map[key] = reinterpret_cast<Value *>(-1);

  pthread_mutex_unlock(&delta_[s].mu);
  return true;
}

// ========== Pin/Unpin（GC 用、RO fast-path で *使用する*） ==========
// (変更なし)
void SimpleStorage::PinEpoch(int64 epoch) {
  auto epochs_map =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
  auto it = epochs_map->find(epoch);
  if (it != epochs_map->end()) {
    it->second->readers.fetch_add(1, std::memory_order_acq_rel);
  }
}

void SimpleStorage::UnpinEpoch(int64 epoch) {
  auto epochs_map =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
  auto it = epochs_map->find(epoch);
  if (it != epochs_map->end()) {
    if (it->second->readers.fetch_sub(1, std::memory_order_acq_rel) == 1) {
      std::lock_guard<std::mutex> lk(hist_mu_);
      hist_cv_.notify_all();
    }
  }
}

bool SimpleStorage::CanRecycleUpTo(int64 epoch) const {
  auto epochs_map =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
  for (auto it = epochs_map->begin();
       it != epochs_map->end() && it->first <= epoch; ++it) {
    if (it->second->readers.load(std::memory_order_acquire) != 0) return false;
  }
  return true;
}

void SimpleStorage::WaitUntilNoReaders(int64 epoch) {
  std::unique_lock<std::mutex> lk(hist_mu_);
  hist_cv_.wait(lk, [&] {
    auto epochs_map =
        std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
    // [★ 修正] epoch *以下* のすべてのリーダーが0になるまで待つ
    auto it = epochs_map->upper_bound(epoch);
    for (auto jt = epochs_map->begin(); jt != it; ++jt) {
      if (jt->second->readers.load(std::memory_order_acquire) != 0) return false;
    }
    return true;
  });
}

// ========== 現在/公開中メタ ==========

int64 SimpleStorage::CurrentEpoch() const {
  auto epochs_map =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
  if (epochs_map->empty()) return 0;
  auto it = epochs_map->end();
  --it;
  return it->first;
}

int64 SimpleStorage::LatestPublishedCut() const {
  return latest_published_cut_.load(std::memory_order_acquire);
}
int64 SimpleStorage::LatestPublishedEpoch() const {
  return latest_published_epoch_.load(std::memory_order_acquire);
}

// cut → epoch の対応を返す（該当するものが無ければ、cut_end ≤ cut の最大 epoch）
int64 SimpleStorage::EpochForCut(int64 cut) const {
  auto epochs_map =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_acquire);
  if (epochs_map->empty()) return 0;

  int64 best_epoch = 0;
  for (const auto &kv : *epochs_map) {
    const int64 ep = kv.first;
    const int64 end = kv.second->cut_end;
    if (end <= cut && ep >= best_epoch) {
      best_epoch = ep;
    }
  }
  return best_epoch;
}

// ========== Prefetch/Unfetch ==========
bool SimpleStorage::Prefetch(const Key & /*key*/, double *wait_time) {
  if (wait_time) *wait_time = 0.0;
  return true;
}
bool SimpleStorage::Unfetch(const Key & /*key*/) { return true; }

// ========== スナップショット公開 ==========
SimpleStorage::SnapshotRequest *SimpleStorage::CaptureDeltasAndCreateRequest(
    int64 new_curr_cut) {
  auto req = new SnapshotRequest();
  req->base_table =
      std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
  req->old_cut = cut_curr_.load(std::memory_order_acquire);
  req->new_cut = new_curr_cut;
  assert(new_curr_cut >= req->old_cut);

  for (size_t s = 0; s < kDeltaShards; ++s) {
    pthread_mutex_lock(&delta_[s].mu);
    if (!delta_[s].map.empty()) {
      Table tmp;
      tmp.swap(delta_[s].map);  // delta を丸ごと引き抜く（高速）
      req->captured_deltas.emplace_back(std::move(tmp));
    }
    pthread_mutex_unlock(&delta_[s].mu);
  }
  return req;
}

void SimpleStorage::ApplyAndPublishSnapshot(SnapshotRequest *req) {
  auto base_table = req->base_table;
  const bool any_delta = !req->captured_deltas.empty();

  // [★ 修正] std::lock_guard を std::unique_lock に変更
  std::unique_lock<std::mutex> lk(hist_mu_);

  auto old_map_ptr =
      std::atomic_load_explicit(&epochs_ptr_, std::memory_order_relaxed);
  auto new_map_ptr = std::make_shared<EpochMap>(*old_map_ptr);

  std::shared_ptr<const Table> next_table;

  if (!any_delta) {
    // 差分なし：base をそのまま次へ
    next_table = base_table;
    // (stable_prev_ などのストア処理は変更なし)
    std::atomic_store_explicit(&stable_prev_, base_table,
                               std::memory_order_release);
    cut_prev_.store(req->old_cut, std::memory_order_release);
    cut_curr_.store(req->new_cut, std::memory_order_release);
  } else {
    // 差分あり：base に captured delta を適用した新テーブルを構築
    // [★ 修正] 新しい Table にも TableDeleter を指定する
    auto next = std::shared_ptr<Table>(new Table(*base_table), TableDeleter());
    
    for (const auto &shard_map : req->captured_deltas) {
      for (const auto &kv : shard_map) {
        // 1. `next` に古い `Value*` があれば `delete`
        auto it = next->find(kv.first);
        if (it != next->end() && it->second != reinterpret_cast<Value *>(-1)) {
          delete it->second;
        }

        if (kv.second == reinterpret_cast<Value *>(-1)) {
          next->erase(kv.first);
        } else {
          // 2. [★ 修正] `delta_` から来た `Value` を *ディープコピー* して `next` に保存
          (*next)[kv.first] = new Value(*kv.second);
        }
      }
    }
    next_table = std::shared_ptr<const Table>(next);

    // (stable_prev_ などのストア処理は変更なし)
    std::atomic_store_explicit(&stable_prev_, base_table,
                               std::memory_order_release);
    std::atomic_store_explicit(&stable_curr_, next_table,
                               std::memory_order_release);
    cut_prev_.store(req->old_cut, std::memory_order_release);
    cut_curr_.store(req->new_cut, std::memory_order_release);
  }

  // エポックを 1 つ進め、RCU マップに (prev,curr) を登録
  const int64 new_epoch = curr_epoch_ + 1;
  (*new_map_ptr)[curr_epoch_] =
      std::make_shared<Snapshot>(base_table, 0, req->old_cut);
  (*new_map_ptr)[new_epoch] =
      std::make_shared<Snapshot>(next_table, 0, req->new_cut);
  
  // [★ 修正] GC対象エポックを決定 (curr_epoch_ の max_history_epochs_ 前)
  int64 epoch_to_gc = -1;
  if (max_history_epochs_ > 0 && new_map_ptr->size() > max_history_epochs_) {
      auto it = new_map_ptr->begin();
      epoch_to_gc = it->first;
  }

  prev_epoch_ = curr_epoch_;
  curr_epoch_ = new_epoch;

  // [★ 修正] GCの前に、RO fast-path が古いエポックを読み終わるのを待つ
  if (epoch_to_gc >= 0) {
      // hist_mu_ を一時的に解放して待機
      lk.unlock(); // [★ 修正] unique_lock なので unlock/lock が可能
      WaitUntilNoReaders(epoch_to_gc);
      lk.lock(); // [★ 修正]
  }

  // 古い世代を GC（読者 0 のもののみ削除）
  // (WaitUntilNoReaders を呼んだので、対象エポックの reader は 0 のはず)
  GCUnlocked(new_map_ptr.get());

  // RCU 切替
  std::atomic_store_explicit(&epochs_ptr_,
                             std::shared_ptr<const EpochMap>(new_map_ptr),
                             std::memory_order_release);

  // 公開メタ更新（RO fast-path が参照）
  latest_published_cut_.store(req->new_cut, std::memory_order_release);
  latest_published_epoch_.store(curr_epoch_, std::memory_order_release);

  // 待機解除（必要な待機者に通知）
  hist_cv_.notify_all();

  // [★ 修正] captured_deltas の Value* (コピー元) をクリーンアップ
  if (any_delta) {
    for (auto& shard_map : req->captured_deltas) {
        for (auto& kv : shard_map) {
            if (kv.second != reinterpret_cast<Value *>(-1)) {
                delete kv.second; // delta が new した Value* をここで delete
            }
        }
    }
  }

  delete req;
}

// ========== GC (hist_mu_ 保持中に呼ぶ) ==========
// [★ 修正] TableDeleter により、map から Snapshot が erase され、
// 参照カウントが 0 になると、TableDeleter が呼ばれて Value* が
// 自動的にクリーンアップされるため、この関数は変更不要。
void SimpleStorage::GCUnlocked(EpochMap *map_to_gc) {
  if (max_history_epochs_ == 0) return;
  while (map_to_gc->size() > max_history_epochs_) {
    auto it = map_to_gc->begin();
    if (it->second->readers.load(std::memory_order_acquire) == 0) {
      map_to_gc->erase(it);
    } else {
      break;
    }
  }
}