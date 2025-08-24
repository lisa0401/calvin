#ifndef _DB_APPLICATIONS_YCSB_H_
#define _DB_APPLICATIONS_YCSB_H_

#include "applications/application.h"
#include "common/random.hh"
#include <vector>
#include <unordered_set> // ★ 変更点 1: <set> から <unordered_set> に変更
#include <atomic>
#include <cstdint>

class YCSB : public Application
{
public:
    YCSB(double read_ratio, double skew, uint64 db_size, uint64 hot_records);

    void InitializeStorage(Storage *storage, Configuration *config) const override;
    TxnProto *NewTxn(int64 txn_id, int txn_type, std::string args, Configuration *config) const override;
    int Execute(TxnProto *txn, StorageManager *storage) const override;

    TxnProto *InitializeTxn() const;
    void PrecomputeKeys(Configuration *config);

private:
    TxnProto *YCSBTxnSP(int64 txn_id, uint32 part, Configuration *config) const;
    TxnProto *YCSBTxnMP(int64 txn_id, uint32 part1, uint32 part2, Configuration *config) const;

    // ★ 変更点 2: 関数の宣言を std::unordered_set に変更
    void GetRandomKeys(std::unordered_set<uint64> &keys, int num_keys, uint32 part) const;

    // パラメータ
    double read_ratio_;
    double skew_;
    uint64 db_size_;
    uint64 hot_records_;

    // 乱数生成器
    mutable Random rnd_;

    // 前計算キャッシュ
    mutable std::atomic<bool> keys_ready_{false};
    std::vector<std::vector<uint64>> keys_per_partition_;
};

#endif // _DB_APPLICATIONS_YCSB_H_