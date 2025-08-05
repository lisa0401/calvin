// backend/storage_manager.cc
#include "backend/storage_manager.h"

#include <set>
#include <map>
#include <memory>
#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "backend/txn_proto_ext.h"

using std::map;
using std::set;
using std::unique_ptr;

StorageManager::StorageManager(Configuration *config, Connection *connection, Storage *actual_storage, TxnProtoExt *txn)
    : txn_(txn),
      configuration_(config),
      connection_(connection),
      actual_storage_(actual_storage),
      reads_sent_(0),
      reads_received_(0)
{
    set<Key> all_reads;
    for (int i = 0; i < txn_->read_set_size(); i++)
    {
        all_reads.insert(txn_->read_set(i));
    }
    for (int i = 0; i < txn_->read_write_set_size(); i++)
    {
        all_reads.insert(txn_->read_write_set(i));
    }

    map<int, MessageProto> remote_requests;

    for (const Key &key : all_reads)
    {
        int partition = configuration_->LookupPartition(key);
        if (partition == configuration_->this_node_id)
        {
            // 修正: ローカル読み込みの場合、直接読むのではなくPrefetchを呼び出す
            double wait_time;
            actual_storage_->Prefetch(key, &wait_time);
            local_keys_.push_back(key); // デストラクタでUnfetchするためにキーを保存
        }
        else
        {
            // リモート読み込みの場合、リクエストをバッチにまとめる
            if (remote_requests.find(partition) == remote_requests.end())
            {
                remote_requests[partition].set_type(MessageProto::TXN_PROTO);
                remote_requests[partition].set_destination_node(partition);
                remote_requests[partition].set_destination_channel(IntToString(txn_->txn_id()));
                remote_requests[partition].set_source_node(configuration_->this_node_id);
            }

            TxnProto remote_read_txn;
            remote_read_txn.add_read_set(key);
            string txn_data;
            remote_read_txn.SerializeToString(&txn_data);
            remote_requests[partition].add_data(txn_data);
        }
    }

    // バッチにまとめたリモート読み込みリクエストを送信
    for (auto const &[partition, message] : remote_requests)
    {
        connection_->Send(message);
        reads_sent_++;
    }
}

StorageManager::~StorageManager()
{
    // 修正: コンストラクタでPrefetchした全てのローカルキーをUnfetchする
    for (const Key &key : local_keys_)
    {
        actual_storage_->Unfetch(key);
    }

    for (auto const &[key, val] : objects_)
    {
        delete val;
    }
    objects_.clear();
}

void StorageManager::CommitWrites()
{
    for (auto const &[key, val] : objects_)
    {
        bool is_write = false;
        for (int i = 0; i < txn_->write_set_size(); i++)
        {
            if (txn_->write_set(i) == key)
            {
                is_write = true;
                break;
            }
        }
        if (!is_write)
        {
            for (int i = 0; i < txn_->read_write_set_size(); i++)
            {
                if (txn_->read_write_set(i) == key)
                {
                    is_write = true;
                    break;
                }
            }
        }

        if (is_write)
        {
            actual_storage_->PutObject(key, new Value(*val), txn_->txn_id());
        }
    }
}

Value *StorageManager::ReadObject(const Key &key)
{
    auto it = objects_.find(key);
    if (it != objects_.end())
    {
        return new Value(*(it->second));
    }

    // コンストラクタでPrefetchが呼ばれているため、このReadObject呼び出しは安全
    Value *fetched_val = actual_storage_->ReadObject(key, txn_->txn_id());
    if (fetched_val)
    {
        objects_[key] = fetched_val;
        return new Value(*fetched_val);
    }

    return nullptr;
}

bool StorageManager::PutObject(const Key &key, Value *value, int64_t txn_id)
{
    if (objects_.count(key))
    {
        delete objects_[key];
    }
    objects_[key] = value;
    return true;
}

bool StorageManager::DeleteObject(const Key &key, int64_t txn_id)
{
    return true;
}

void StorageManager::HandleReadResult(const MessageProto &message)
{
    assert(message.type() == MessageProto::READ_RESULT);
    for (int i = 0; i < message.keys_size(); i++)
    {
        const string &key = message.keys(i);
        Value *val = new Value();
        val->data = message.values(i);

        if (objects_.count(key))
        {
            delete objects_[key];
        }
        objects_[key] = val;
    }
    reads_received_++;
}

bool StorageManager::ReadyToExecute()
{
    return reads_sent_ == reads_received_;
}
