// FILE: ycsb.cc

#include "applications/ycsb.h"

#include <string>
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/configuration.h"
#include "common/utils.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"

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
}

TxnProto *YCSB::NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const
{
    const double mp_ratio = 0.01;
    if (config->all_nodes.size() == 1 || rnd_.NextUniform() >= mp_ratio)
    {
        uint32 part = rnd_.Uniform(config->all_nodes.size());
        return YCSBTxnSP(txn_id, part, config);
    }
    else
    {
        uint32 part1 = rnd_.Uniform(config->all_nodes.size());
        uint32 part2;
        do
        {
            part2 = rnd_.Uniform(config->all_nodes.size());
        } while (part1 == part2);
        return YCSBTxnMP(txn_id, part1, part2, config);
    }
}

int YCSB::Execute(TxnProto *txn, StorageManager *storage) const
{
    return 0;
}

void YCSB::GetRandomKeys(std::set<uint64> &keys, int num_keys, uint64 key_start,
                         uint64 key_limit, uint32 part, bool is_uniform, Configuration *config) const
{
    keys.clear();
    while (keys.size() < (uint32)num_keys)
    {
        uint64 key = is_uniform ? key_start + rnd_.Uniform(key_limit - key_start)
                                : key_start + rnd_.Zipf(key_limit - key_start, SKEW);

        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        // --- MODIFICATION: Explicitly cast both sides to uint32 to resolve warning/error ---
        if (static_cast<uint32>(config->LookupPartition(key_str)) == part)
        {
            if (keys.find(key) == keys.end())
            {
                keys.insert(key);
            }
        }
    }
}

TxnProto *YCSB::YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const
{
    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_SP);

    bool is_read = (rnd_.NextUniform()) < read_ratio_;
    uint64 hotkey = rnd_.Uniform(hot_records_);
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

    const double uniform_ratio = UNIFORM_KEY_SELECTION_RATIO;
    bool is_uniform = (rnd_.Uniform(100)) < uniform_ratio;

    std::set<uint64> keys;
    GetRandomKeys(keys, RW_SET_SIZE - 1, hot_records_, db_size_, part, is_uniform, config);
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
    TxnProto *txn = new TxnProto();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_MP);
    txn->set_multipartition(true);

    bool is_read = (rnd_.NextUniform()) < read_ratio_;
    uint64 hotkey1 = rnd_.Uniform(hot_records_);
    uint64 hotkey2 = rnd_.Uniform(hot_records_);
    char hotkey1_str[32], hotkey2_str[32];
    snprintf(hotkey1_str, sizeof(hotkey1_str), "k%lu", hotkey1);
    // --- MODIFICATION: Fix typo from sizeof(key_str) to sizeof(hotkey2_str) ---
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

    const double uniform_ratio = UNIFORM_KEY_SELECTION_RATIO;
    bool is_uniform = (rnd_.Uniform(100)) < uniform_ratio;
    std::set<uint64> keys;

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, hot_records_, db_size_, part1, is_uniform, config);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        is_read ? txn->add_read_set(key_str) : txn->add_read_write_set(key_str);
    }

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, hot_records_, db_size_, part2, is_uniform, config);
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