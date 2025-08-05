// FILE: ycsb.h

#ifndef _DB_APPLICATIONS_YCSB_H_
#define _DB_APPLICATIONS_YCSB_H_

#include <set>
#include <string>

#include "applications/application.h"
#include "common/random.hh"
#include "common/zipf.hh"
#include "common/types.h" // int64, uint64 のためにインクルード

// Forward declarations
class StorageManager;
class Storage;
class Configuration;
class TxnProtoExt;

using std::set;
using std::string;

class YCSB : public Application
{
public:
    // Transaction types.
    enum TxnType
    {
        INITIALIZE = 0,
        YCSB_TXN_SP = 1, // Single-partition YCSB transaction
        YCSB_TXN_MP = 2, // Multi-partition YCSB transaction
    };

    // Constructor: Initializes random number generators.
    YCSB();
    virtual ~YCSB() {}

    virtual void InitializeStorage(Storage *storage, Configuration *config) const;

    virtual TxnProtoExt *NewTxn(int64 txn_id, int txn_type, string args,
                                Configuration *config) const;

    virtual int Execute(TxnProtoExt *txn, StorageManager *storage) const;

private:
    // Generates a new initialization transaction.
    TxnProtoExt *InitializeTxn() const;

    // Generates a new single-partition transaction.
    TxnProtoExt *YCSBTxnSP(int64 txn_id, int part, Configuration *config) const;

    // Generates a new multi-partition transaction.
    TxnProtoExt *YCSBTxnMP(int64 txn_id, int part1, int part2, Configuration *config) const;

    // Fills 'keys' with 'num_keys' distinct keys from the specified key range and partition.
    void GetRandomKeys(set<uint64> &keys, int num_keys, int key_start,
                       int key_limit, int part, bool is_uniform, Configuration *config) const;

    // Random number generator and Zipfian distribution generator.
    // 'mutable' allows them to be modified even in const methods.
    mutable Xoroshiro128Plus rnd_;
    mutable FastZipf zipf_;
};

#endif // _DB_APPLICATIONS_YCSB_H_