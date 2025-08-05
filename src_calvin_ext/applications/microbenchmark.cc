// Author: Kun Ren (kun.ren@yale.edu)
// Author: Alexander Thomson (thomson@cs.yale.edu)
//
// TODO(scw): remove iostream, use cstdio instead

#include "applications/microbenchmark.h"

#include <iostream>
#include <memory>
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "common/utils.h"
#include "common/configuration.h"
#include "common/definitions.hh"
#include "proto/txn.pb.h"
#include "backend/txn_proto_ext.h"

// #define PREFETCHING

// Fills '*keys' with num_keys unique ints k where
// 'key_start' <= k < 'key_limit', and k == part (mod nparts).
// Requires: key_start % nparts == 0
void Microbenchmark::GetRandomKeys(set<int> *keys,
                                   int num_keys,
                                   int key_start,
                                   int key_limit,
                                   int part,
                                   bool is_uniform)
{
    assert(key_start % nparts == 0);
    keys->clear();
    for (int i = 0; i < num_keys; i++)
    {
        // Find a key not already in '*keys'.
        int key;
        do
        {
            key = key_start + part +
                  nparts * (((is_uniform) ? rand() : zipf_()) %
                            ((key_limit - key_start) / nparts));
        } while (keys->count(key));
        keys->insert(key);
    }
}

TxnProtoExt *Microbenchmark::InitializeTxn()
{
    // Create the new transaction object
    TxnProtoExt *txn = new TxnProtoExt();

    // Set the transaction's standard attributes
    txn->set_txn_id(0);
    txn->set_txn_type(INITIALIZE);

    // Nothing read, everything written.
    for (int i = 0; i < DB_SIZE; i++)
        txn->add_write_set(IntToString(i));

    return txn;
}

// Create a non-dependent single-partition transaction
TxnProtoExt *Microbenchmark::MicroTxnSP(int64 txn_id, int part)
{
    // Create the new transaction object
    TxnProtoExt *txn = new TxnProtoExt();

    // Set the transaction's standard attributes
    txn->set_txn_id(txn_id);
    txn->set_txn_type(MICROTXN_SP);

    // // Add one hot key to read/write set.
    // int hotkey = part + nparts * (rand() % hot_records);
    // txn->add_read_write_set(IntToString(hotkey));

    bool is_uniform = (rand() % 100) < UNIFORM_KEY_SELECTION_RATIO;

    // Insert set of RW_SET_SIZE - 1 random cold keys from specified partition
    // into read/write set.
    set<int> keys;
    GetRandomKeys(&keys, RW_SET_SIZE, nparts * hot_records, nparts * DB_SIZE,
                  part, is_uniform); // uniform dist
    for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
        txn->add_read_write_set(IntToString(*it));

    return txn;
}

// Create a non-dependent multi-partition transaction
TxnProtoExt *Microbenchmark::MicroTxnMP(int64 txn_id, int part1, int part2)
{
    assert(part1 != part2 || nparts == 1);
    // Create the new transaction object
    TxnProtoExt *txn = new TxnProtoExt();

    // Set the transaction's standard attributes
    txn->set_txn_id(txn_id);
    txn->set_txn_type(MICROTXN_MP);

    // Add two hot keys to read/write set---one in each partition.
    int hotkey1 = part1 + nparts * (rand() % hot_records);
    int hotkey2 = part2 + nparts * (rand() % hot_records);
    txn->add_read_write_set(IntToString(hotkey1));
    txn->add_read_write_set(IntToString(hotkey2));

    bool is_uniform = (rand() % 100) < UNIFORM_KEY_SELECTION_RATIO;

    // Insert set of RW_SET_SIZE/2 - 1 random cold keys from each partition into
    // read/write set.
    set<int> keys;
    GetRandomKeys(&keys, RW_SET_SIZE / 2 - 1, nparts * hot_records,
                  nparts * DB_SIZE, part1, is_uniform);
    for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
        txn->add_read_write_set(IntToString(*it));
    GetRandomKeys(&keys, RW_SET_SIZE / 2 - 1, nparts * hot_records,
                  nparts * DB_SIZE, part2, is_uniform);
    for (set<int>::iterator it = keys.begin(); it != keys.end(); ++it)
        txn->add_read_write_set(IntToString(*it));

    return txn;
}

// The load generator can be called externally to return a transaction proto
// containing a new type of transaction.
TxnProtoExt *Microbenchmark::NewTxn(int64 txn_id,
                                    int txn_type,
                                    string args,
                                    Configuration *config) const
{
    return NULL;
}

int Microbenchmark::Execute(TxnProtoExt *txn, StorageManager *storage) const
{
    // Microbenchmark のアプリケーション固有のロジックを必要とするならここに記述。
    // 例: 特定のキーを読み込むなどの事前処理
    // for (int i = 0; i < txn->read_set_size(); ++i) {
    //     const string &key = txn->read_set(i);
    //     Value *val = storage->ReadObject(key); // これで val が未使用にならないように使う
    //     // 例: printf("Reading key %s with version %d\n", key.c_str(), val->version);
    // }

    // 最終的に、TxnProtoExt の Execute メソッドに処理を委譲する。
    // このメソッドが AddReadVersions, Validate, ApplyWrites を全て処理します。
    return txn->Execute(storage, this);
}

void Microbenchmark::InitializeStorage(Storage *storage, Configuration *conf) const
{
    for (int i = 0; i < nparts * DB_SIZE; i++)
    {
        if (conf->LookupPartition(IntToString(i)) == conf->this_node_id)
        {
            Value *initial_val = new Value(IntToString(i));
            initial_val->version = 1;
            storage->PutObject(IntToString(i), initial_val, initial_val->version);
        }
    }
}
