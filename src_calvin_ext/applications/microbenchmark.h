// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// A microbenchmark application that reads all elements of the read_set, does
// some trivial computation, and writes to all elements of the write_set.

#ifndef _DB_APPLICATIONS_MICROBENCHMARK_H_
#define _DB_APPLICATIONS_MICROBENCHMARK_H_

#include <set>
#include <string>
#include <memory> // ★★★ std::unique_ptr のために追加 ★★★

#include "applications/application.h"
#include "common/definitions.hh"
#include "common/random.hh"
#include "common/zipf.hh"

// Value struct の前方宣言、または common/types.h のインクルード
// (common/types.h が Value struct を定義している場合、そちらをインクルード)
#include "common/types.h"

using std::set;
using std::string;

// StorageManager の前方宣言
class StorageManager;
class Storage;
class Configuration;
class TxnProtoExt; // TxnProtoExt の前方宣言

class Microbenchmark : public Application
{
public:
    Xoroshiro128Plus rnd_;
    FastZipf zipf_;

    enum TxnType
    {
        INITIALIZE = 0,
        MICROTXN_SP = 1,
        MICROTXN_MP = 2,
    };

    Microbenchmark(int nodecount, int hotcount)
        : rnd_(), zipf_(&rnd_, SKEW, DB_SIZE)
    {
        nparts = nodecount;
        hot_records = hotcount;
    }

    virtual ~Microbenchmark() {}

    virtual TxnProtoExt *NewTxn(int64 txn_id,
                                int txn_type,
                                string args,
                                Configuration *config = NULL) const;

    // Execute のシグネチャは合っているようです (TxnProtoExt* はOK)
    virtual int Execute(TxnProtoExt *txn, StorageManager *storage) const;

    TxnProtoExt *InitializeTxn();
    TxnProtoExt *MicroTxnSP(int64 txn_id, int part);
    TxnProtoExt *MicroTxnMP(int64 txn_id, int part1, int part2);

    int nparts;
    int hot_records;

    // InitializeStorage の const を削除 (実装に合わせる)
    virtual void InitializeStorage(Storage *storage, Configuration *conf) const;

private:
    void GetRandomKeys(set<int> *keys,
                       int num_keys,
                       int key_start,
                       int key_limit,
                       int part,
                       bool is_uniform);
    Microbenchmark() : rnd_(), zipf_(&rnd_, SKEW, DB_SIZE) {}
};

#endif // _DB_APPLICATIONS_MICROBENCHMARK_H_