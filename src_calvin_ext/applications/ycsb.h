// applications/ycsb.h

#ifndef _DB_APPLICATIONS_YCSB_H_
#define _DB_APPLICATIONS_YCSB_H_

#include "applications/application.h"
#include <vector>
#include <set>
// ★ 変更点 1: 新しい高速な乱数生成器のヘッダをインクルード
#include "common/random.hh"
#include <atomic>
#include <iostream>
class YCSB : public Application
{
public:
    YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records);
    virtual void InitializeStorage(Storage *storage, Configuration *config) const;
    virtual TxnProto *NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const;
    virtual int Execute(TxnProto *txn, StorageManager *storage) const;
    TxnProto *InitializeTxn() const;

    void PrecomputeKeys(Configuration *config);

private:
    TxnProto *YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const;
    TxnProto *YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2, Configuration *config) const;

    void GetRandomKeys(std::set<uint64> &keys, int num_keys, uint32 part) const;

    double read_ratio_;
    uint64 db_size_;
    uint64 hot_records_;

    // ★ 変更点 2: メンバー変数の型をRandomからXoroshiro128Plusに変更
    mutable Xoroshiro128Plus rnd_;

    std::vector<std::vector<uint64>> keys_per_partition_;
    mutable std::atomic<bool> keys_ready_{false};
};

#endif // _DB_APPLICATIONS_YCSB_H_