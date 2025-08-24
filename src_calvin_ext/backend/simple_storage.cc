#include "backend/simple_storage.h"
#include <utility>

SimpleStorage::SimpleStorage()
    : delta_(kDeltaShards),
      stable_prev_(std::make_shared<Table>()),
      stable_curr_(std::make_shared<Table>()),
      cut_prev_(-1),
      cut_curr_(-1) {}

// --- 読取り（ROは完全ロックレス）：
// txn_id <= prev_cut → prev を参照
// txn_id <= curr_cut → curr を参照
// それ以外（最新系＝RWなど）→ delta(シャードを軽ロック) → curr
Value *SimpleStorage::ReadObject(const Key &key, int64 txn_id)
{
    const int64 prev_cut = cut_prev_.load(std::memory_order_acquire);
    const int64 curr_cut = cut_curr_.load(std::memory_order_acquire);

    if (txn_id >= 0 && txn_id <= prev_cut)
    {
        auto sp = std::atomic_load_explicit(&stable_prev_, std::memory_order_acquire);
        auto it = sp->find(key);
        return (it == sp->end() || it->second == Tombstone()) ? nullptr : it->second;
    }

    if (txn_id >= 0 && txn_id <= curr_cut)
    {
        auto sc = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);
        auto it = sc->find(key);
        return (it == sc->end() || it->second == Tombstone()) ? nullptr : it->second;
    }

    // 最新系：まず delta を見る（該当シャードのみロック）
    {
        const size_t s = ShardFor(key);
        pthread_mutex_lock(&delta_[s].mu);
        auto it = delta_[s].map.find(key);
        if (it != delta_[s].map.end())
        {
            Value *v = it->second;
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

// --- 書込み：シャード単位で in-place 更新（コピーもCASループも無し）---
bool SimpleStorage::PutObject(const Key &key, Value *value, int64 /*txn_id*/)
{
    const size_t s = ShardFor(key);
    pthread_mutex_lock(&delta_[s].mu);
    delta_[s].map[key] = value;
    pthread_mutex_unlock(&delta_[s].mu);
    return true;
}

bool SimpleStorage::DeleteObject(const Key &key, int64 /*txn_id*/)
{
    const size_t s = ShardFor(key);
    pthread_mutex_lock(&delta_[s].mu);
    delta_[s].map[key] = Tombstone();
    pthread_mutex_unlock(&delta_[s].mu);
    return true;
}

// --- スナップショット公開：各シャードを swap で奪取して curr に一括マージ ---
void SimpleStorage::PublishSnapshot(int64 new_curr_cut)
{
    // いま公開中の curr（ROが参照中のもの）
    auto curr = std::atomic_load_explicit(&stable_curr_, std::memory_order_acquire);

    // delta を奪取（各シャードの map を O(1) swap）
    bool any_delta = false;
    std::vector<Table> captured;
    captured.reserve(kDeltaShards);
    for (size_t s = 0; s < kDeltaShards; ++s)
    {
        Table tmp;
        pthread_mutex_lock(&delta_[s].mu);
        if (!delta_[s].map.empty())
        {
            any_delta = true;
            tmp.swap(delta_[s].map);
        }
        pthread_mutex_unlock(&delta_[s].mu);
        if (!tmp.empty())
            captured.emplace_back(std::move(tmp));
    }

    if (!any_delta)
    {
        // 変更なし：prev <- curr（ポインタ再掲）、cut を前進
        std::atomic_store_explicit(&stable_prev_, curr, std::memory_order_release);
        const int64 old_curr_cut = cut_curr_.load(std::memory_order_acquire);
        cut_prev_.store(old_curr_cut, std::memory_order_release);
        cut_curr_.store(new_curr_cut, std::memory_order_release);
        return;
    }

    // 変更あり：curr をクローンして delta を適用
    auto next = std::make_shared<Table>(*curr);
    for (const auto &shard_map : captured)
    {
        for (const auto &kv : shard_map)
        {
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
    const int64 old_curr_cut = cut_curr_.load(std::memory_order_acquire);
    cut_prev_.store(old_curr_cut, std::memory_order_release);
    cut_curr_.store(new_curr_cut, std::memory_order_release);
}
