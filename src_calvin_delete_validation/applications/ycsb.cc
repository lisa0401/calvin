// FILE: ycsb.cc

#include "applications/ycsb.h"

#include <string>
#include "backend/storage.h" // Storageクラスの完全な定義のために追加
#include "backend/storage_manager.h"
#include "backend/txn_proto_ext.h"
#include "common/configuration.h"
#include "common/definitions.hh"
#include "common/utils.h"

// definitions.hhで定義されていないYCSB固有のマクロを定義
#define MP_RATIO 0.01
#define READ_RATIO 0.95

YCSB::YCSB() : rnd_(), zipf_(&rnd_, SKEW, DB_SIZE - HOT_RECORDS) {}

void YCSB::InitializeStorage(Storage *storage,
                             Configuration *config) const
{
    for (uint64 i = 0; i < DB_SIZE; i++)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        if (config->LookupPartition(key_str) == config->this_node_id)
        {
            Value *value = new Value(RandomString(128));
            storage->PutObject(key_str, value, 1);
        }
    }
}

TxnProtoExt *YCSB::NewTxn(int64 txn_id, int txn_type, string args,
                          Configuration *config) const
{
    if (txn_type == INITIALIZE)
    {
        return InitializeTxn();
    }

    if (config->all_nodes.size() == 1 || (double)rand() / RAND_MAX >= MP_RATIO)
    {
        int part = rand() % config->all_nodes.size();
        return YCSBTxnSP(txn_id, part, config);
    }
    else
    {
        int part1 = rand() % config->all_nodes.size();
        int part2;
        do
        {
            part2 = rand() % config->all_nodes.size();
        } while (part1 == part2);
        return YCSBTxnMP(txn_id, part1, part2, config);
    }
    return nullptr; // Should not be reached
}

int YCSB::Execute(TxnProtoExt *txn, StorageManager *storage) const
{
    return txn->Execute(storage, this);
}

void YCSB::GetRandomKeys(set<uint64> &keys, int num_keys, int key_start,
                         int key_limit, int part, bool is_uniform, Configuration *config) const
{
    keys.clear();
    while (keys.size() < (uint32)num_keys)
    {
        uint64 key;
        if (is_uniform)
        {
            key = key_start + (rand() % (key_limit - key_start));
        }
        else
        {
            key = key_start + (zipf_() - 1); // FastZipfは()で呼び出す
        }

        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (config->LookupPartition(key_str) == static_cast<uint32>(part))
        { // 符号付き/なしの比較警告を修正
            if (keys.find(key) == keys.end())
            {
                keys.insert(key);
            }
        }
    }
}

TxnProtoExt *YCSB::InitializeTxn() const
{
    TxnProtoExt *txn = new TxnProtoExt();
    txn->set_txn_id(0);
    txn->set_txn_type(INITIALIZE);
    for (uint64 i = 0; i < DB_SIZE; i++)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", i);
        txn->add_write_set(key_str);
    }
    return txn;
}

TxnProtoExt *YCSB::YCSBTxnSP(int64 txn_id, int part, Configuration *config) const
{
    TxnProtoExt *txn = new TxnProtoExt();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_SP);

    bool is_read = ((double)rand() / RAND_MAX) < READ_RATIO;

    uint64 hotkey = rand() % HOT_RECORDS;
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

    bool is_uniform = (rand() % 100) < UNIFORM_KEY_SELECTION_RATIO;
    set<uint64> keys;
    GetRandomKeys(keys, RW_SET_SIZE - 1, HOT_RECORDS, DB_SIZE, part, is_uniform, config); // Application::Config()をやめてconfigを渡す
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }

    return txn;
}

TxnProtoExt *YCSB::YCSBTxnMP(int64 txn_id, int part1, int part2, Configuration *config) const
{
    TxnProtoExt *txn = new TxnProtoExt();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(YCSB_TXN_MP);
    txn->set_multipartition(true);

    bool is_read = ((double)rand() / RAND_MAX) < READ_RATIO;

    uint64 hotkey1 = rand() % HOT_RECORDS;
    uint64 hotkey2 = rand() % HOT_RECORDS;
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

    bool is_uniform = (rand() % 100) < UNIFORM_KEY_SELECTION_RATIO;
    set<uint64> keys;

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, HOT_RECORDS, DB_SIZE, part1, is_uniform, config);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }

    GetRandomKeys(keys, RW_SET_SIZE / 2 - 1, HOT_RECORDS, DB_SIZE, part2, is_uniform, config);
    for (uint64 key : keys)
    {
        char key_str[32];
        snprintf(key_str, sizeof(key_str), "k%lu", key);
        if (is_read)
        {
            txn->add_read_set(key_str);
        }
        else
        {
            txn->add_read_write_set(key_str);
        }
    }

    return txn;
}
