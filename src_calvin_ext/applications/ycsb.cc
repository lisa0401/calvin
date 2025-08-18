// FILE: applications/ycsb.cc

#include "applications/ycsb.h"
#include <iostream>
#include <string>
#include <vector>
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include <atomic>

enum YCSBTxnType
{
    YCSB_TXN_SP,
    YCSB_TXN_MP,
    YCSB_TXN_INITIALIZE
};

YCSB::YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records)
    : read_ratio_(read_ratio),
      db_size_(db_size),
      hot_records_(hot_records),
      rnd_() {}

void YCSB::InitializeStorage(Storage *storage, Configuration *config) const
{
    for (uint64 i = 0; i < db_size_; i++)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        if (config->LookupPartition(key_str) == config->this_node_id)
        {
            Value *value = new Value(RandomString(128), 0);
            storage->PutObject(key_str, value);
        }
    }
    // ここで前計算しておく（初回遅延初期化でもOK）
    const_cast<YCSB *>(this)->PrecomputeKeys(config);
}

// 事前計算
void YCSB::PrecomputeKeys(Configuration *config)
{
    keys_per_partition_.clear();
    keys_per_partition_.resize(config->all_nodes.size());
    for (uint64_t i = hot_records_; i < db_size_; ++i)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        int partition_id = config->LookupPartition(key_str);
        keys_per_partition_[partition_id].push_back(i);
    }
    keys_ready_.store(true, std::memory_order_release);
    std::cout << "Key precomputation complete." << std::endl;
}

TxnProto *YCSB::NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const
{
    const double mp_ratio = 0.01;
    if (config->all_nodes.size() == 1 || rnd_.NextUniform() >= mp_ratio)
    {
        uint32 part = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        return YCSBTxnSP(txn_id, part, config);
    }
    else
    {
        uint32 part1 = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        uint32 part2;
        do
        {
            part2 = rnd_.Uniform(static_cast<int>(config->all_nodes.size()));
        } while (part1 == part2);
        return YCSBTxnMP(txn_id, part1, part2, config);
    }
}

int YCSB::Execute(TxnProto *txn, StorageManager *storage) const
{
    return 0;
}

// ランダムキー抽選（前計算リストから重複ナシで取り出し）
void YCSB::GetRandomKeys(std::set<uint64> &keys, int num_keys, uint32 part) const
{
    keys.clear();
    if (!keys_ready_.load(std::memory_order_acquire))
        return;
    const auto &key_candidates = keys_per_partition_[part];
    if (key_candidates.empty())
        return;
    while ((int)keys.size() < num_keys)
    {
        uint64_t index = rnd_.Uniform(static_cast<int>(key_candidates.size()));
        keys.insert(key_candidates[index]);
    }
}

TxnProto *YCSB::YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const
{
    if (!keys_ready_.load(std::memory_order_acquire))
        const_cast<YCSB *>(this)->PrecomputeKeys(config);

    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_SP);

    bool is_read = (rnd_.NextUniform()) < read_ratio_;

    // ホットキーを1つ選ぶ
    uint64 hotkey = rnd_.Uniform(static_cast<int>(hot_records_));
    char hotkey_str[32];
    snprintf(hotkey_str, sizeof(hotkey_str), "k%lu", hotkey);
    if (is_read)
    {
        txn->add_read_set(hotkey_str);
    }
    else
    {
        txn->add_read_write_set(hotkey_str);
    }

    std::set<uint64> keys;
    GetRandomKeys(keys, RW_SET_SIZE - 1, part);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        is_read ? txn->add_read_set(key_str) : txn->add_read_write_set(key_str);
    }
    return txn;
}

TxnProto *YCSB::YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2, Configuration *config) const
{
    if (!keys_ready_.load(std::memory_order_acquire))
        const_cast<YCSB *>(this)->PrecomputeKeys(config);

    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_MP);
    txn->set_multipartition(true);

    bool is_read = (rnd_.NextUniform()) < read_ratio_;

    // 各パーティションからホットキー
    uint64 hotkey1 = rnd_.Uniform(static_cast<int>(hot_records_));
    uint64 hotkey2 = rnd_.Uniform(static_cast<int>(hot_records_));
    char hotkey1_str[32], hotkey2_str[32];
    snprintf(hotkey1_str, sizeof(hotkey1_str), "k%lu", hotkey1);
    snprintf(hotkey2_str, sizeof(hotkey2_str), "k%lu", hotkey2);
    if (is_read)
    {
        txn->add_read_set(hotkey1_str);
        txn->add_read_set(hotkey2_str);
    }
    else
    {
        txn->add_read_write_set(hotkey1_str);
        txn->add_read_write_set(hotkey2_str);
    }

    std::set<uint64> keys;

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, part1);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        is_read ? txn->add_read_set(key_str) : txn->add_read_write_set(key_str);
    }

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, part2);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        is_read ? txn->add_read_set(key_str) : txn->add_read_write_set(key_str);
    }

    return txn;
}

TxnProto *YCSB::InitializeTxn() const
{
    TxnProto *txn = new TxnProto();
    txn->set_txn_type(YCSB_TXN_INITIALIZE);
    return txn;
}
