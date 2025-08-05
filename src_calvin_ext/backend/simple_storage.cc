#include "backend/simple_storage.h"
#include <iostream> // For debug cerr

// Constructor to initialize the mutex
SimpleStorage::SimpleStorage()
{
    pthread_mutex_init(&mutex_, NULL);
}

// Destructor to destroy the mutex and free remaining objects
SimpleStorage::~SimpleStorage()
{
    pthread_mutex_destroy(&mutex_);
    // Clean up any remaining Value* objects in the map to prevent memory leaks
    for (std::map<Key, Value *>::iterator it = objects_.begin();
         it != objects_.end(); ++it)
    {
        delete it->second;
    }
    objects_.clear();
}

Value *SimpleStorage::ReadObject(const Key &key, int64 txn_id)
{
    pthread_mutex_lock(&mutex_); // Read operations should also be protected

    auto it = objects_.find(key);
    if (it != objects_.end())
    {
        // ★修正: 内部オブジェクトのコピーを返す (所有権を渡す)
        //         呼び出し元がこのコピーを delete する責任を持つ
        Value *copied_value = new Value(*(it->second)); // Deep copy
        pthread_mutex_unlock(&mutex_);
        return copied_value;
    }
    else
    {
        pthread_mutex_unlock(&mutex_);
        return NULL;
    }
}

bool SimpleStorage::PutObject(const Key &key, Value *value, int64 txn_id)
{
    pthread_mutex_lock(&mutex_);

    // ★修正: 既存のオブジェクトがあれば解放する (メモリリーク防止)
    if (objects_.count(key) > 0)
    {
        delete objects_[key]; // 古い Value* を解放
    }

    // ★修正: バージョン管理ロジックはアプリケーション (TPCC::ApplyWrites) または
    //         より上位のストレージマネージャに任せるべき。
    //         SimpleStorage は渡された value のバージョンをそのまま保存する。
    //         TPCC::ApplyWrites で version++ されているので、それを信頼する。
    //         PutObject の第3引数 (version) は、ここでは無視されるか、
    //         整合性チェックのために使用されるべき。
    //         ここでは、渡された value の version をそのまま使う。

    objects_[key] = value; // ★渡された Value* の所有権を SimpleStorage が持つ

    pthread_mutex_unlock(&mutex_);
    return true;
}

bool SimpleStorage::DeleteObject(const Key &key, int64 txn_id)
{
    pthread_mutex_lock(&mutex_);
    auto it = objects_.find(key);
    if (it != objects_.end())
    {
        delete it->second; // 削除する Value* を解放
        objects_.erase(it);
    }
    pthread_mutex_unlock(&mutex_);
    return true;
}

// Initmutex はコンストラクタで行うため不要
// void SimpleStorage::Initmutex() {
//     pthread_mutex_init(&mutex_, NULL);
// }

bool SimpleStorage::Prefetch(const Key &key, double *wait_time)
{
    // SimpleStorage does not implement complex prefetching.
    // Just return true or false, or do nothing.
    // For a basic implementation, assume it's always "available" instantly.
    if (wait_time)
        *wait_time = 0.0; // Indicate no wait if pointer is valid
    return true;          // Assume Prefetch always succeeds for SimpleStorage
}

bool SimpleStorage::Unfetch(const Key &key)
{
    // SimpleStorage does not implement complex unfetching.
    // Just return true or false, or do nothing.
    return true; // Assume Unfetch always succeeds for SimpleStorage
}