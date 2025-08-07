#ifndef _DB_APPLICATIONS_YCSB_H_
#define _DB_APPLICATIONS_YCSB_H_

#include "applications/application.h"
#include "common/random.hh"
#include <string>
#include <vector>
#include <set>

class TxnProto;

class YCSB : public Application
{
public:
    YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records);
    virtual ~YCSB() {}

    virtual void InitializeStorage(Storage *storage, Configuration *conf) const;
    virtual TxnProto *NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const;
    virtual int Execute(TxnProto *txn, StorageManager *storage) const;

private:
    TxnProto *InitializeTxn() const;
    TxnProto *YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const;
    TxnProto *YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2, Configuration *config) const;
    void GetRandomKeys(std::set<uint64> &keys, int num_keys, uint64 key_start,
                       uint64 key_limit, uint32 part, bool is_uniform, Configuration *config) const;

    double read_ratio_;
    uint64 db_size_;
    uint64 hot_records_;

    mutable Xoroshiro128Plus rnd_;
};

#endif // _DB_APPLICATIONS_YCSB_H_