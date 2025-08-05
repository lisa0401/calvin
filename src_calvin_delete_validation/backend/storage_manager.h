// backend/storage_manager.h
#ifndef _DB_BACKEND_STORAGE_MANAGER_H_
#define _DB_BACKEND_STORAGE_MANAGER_H_

#include "common/types.h"
#include <string>
#include <tr1/unordered_map>
#include <vector>

class Configuration;
class Connection;
class Storage;
class TxnProtoExt;
class Value;
class MessageProto;

class StorageManager
{
public:
    StorageManager(Configuration *config, Connection *connection, Storage *actual_storage, TxnProtoExt *txn);
    ~StorageManager();

    Value *ReadObject(const Key &key);
    bool PutObject(const Key &key, Value *value, int64_t txn_id);
    bool DeleteObject(const Key &key, int64_t txn_id);

    void HandleReadResult(const MessageProto &message);
    bool ReadyToExecute();

    void CommitWrites();

    TxnProtoExt *txn_;

private:
    Configuration *configuration_;
    Connection *connection_;
    Storage *actual_storage_;

    std::tr1::unordered_map<Key, Value *> objects_;

    int reads_sent_;
    int reads_received_;

    // 修正: 先読みしたローカルキーを追跡するためのベクトルを追加
    std::vector<Key> local_keys_;
};

#endif // _DB_BACKEND_STORAGE_MANAGER_H_

// // Author: Alexander Thomson (thomson@cs.yale.edu)
// // Author: Kun Ren (kun@cs.yale.edu)
// //
// // A wrapper for a storage layer that can be used by an Application to simplify
// // application code by hiding all inter-node communication logic.
// #ifndef _DB_BACKEND_STORAGE_MANAGER_H_
// #define _DB_BACKEND_STORAGE_MANAGER_H_

// #include <ucontext.h>

// #include <tr1/unordered_map>
// #include <vector>
// #include <memory> // std::unique_ptr を使用するために追加

// #include "common/types.h"

// using std::vector;
// using std::tr1::unordered_map;

// class Configuration;
// class Connection;
// class MessageProto;
// class Scheduler;
// class Storage;
// class TxnProtoExt;

// class StorageManager
// {
// public:
//     // TODO(alex): Document this class correctly.
//     // 既存のコンストラクタ
//     StorageManager(Configuration *config,
//                    Connection *connection,
//                    Storage *actual_storage,
//                    TxnProtoExt *txn);

//     // ★修正: 新しいコンストラクタの宣言を追加
//     StorageManager(Configuration *config,
//                    Connection *connection,
//                    Storage *actual_storage);

//     ~StorageManager();
//     Value *GetValue(const std::string &key);

//     Value *ReadObject(const Key &key);
//     // storage_manager.h に追加
//     Value *GenerateWriteValue(const std::string &key);
//     void Put(const std::string &key, Value *val);

//     bool PutObject(const Key &key, Value *value, int64 txn_id); // ★修正：versionをtxn_idに
//     bool DeleteObject(const Key &key, int64 txn_id);

//     void HandleReadResult(const MessageProto &message);
//     bool ReadyToExecute();

//     Storage *GetStorage() { return actual_storage_; }

//     // Set by the constructor, indicating whether 'txn' involves any writes at
//     // this node.
//     bool writer;

//     // private:
//     friend class DeterministicScheduler;

//     // Pointer to the configuration object for this node.
//     Configuration *configuration_;

//     // A Connection object that can be used to send and receive messages.
//     Connection *connection_;

//     // Storage layer that *actually* stores data objects on this node.
//     Storage *actual_storage_;

//     // Transaction that corresponds to this instance of a StorageManager.
//     TxnProtoExt *txn_;

//     // Local copy of all data objects read/written by 'txn_', populated at
//     // StorageManager construction time.
//     unordered_map<Key, Value *> objects_;

//     vector<Value *> remote_reads_;
// };

// #endif // _DB_BACKEND_STORAGE_MANAGER_H_