// backend/simple_storage.h
#pragma once
#ifndef _DB_BACKEND_SIMPLE_STORAGE_H_
#define _DB_BACKEND_SIMPLE_STORAGE_H_

#include <string>
#include <map>
#include <pthread.h>

#include "backend/storage.h"

// For testing purposes only.

class SimpleStorage : public Storage
{
public:
    // ★コンストラクタの宣言を追加
    SimpleStorage();

    // ★デストラクタの定義 {} を削除し、宣言のみにする
    virtual ~SimpleStorage(); // {} を削除

    virtual Value *ReadObject(const Key &key, int64 txn_id = 0);
    virtual bool PutObject(const Key &key, Value *value, int64 txn_id = 0);
    virtual bool DeleteObject(const Key &key, int64 txn_id = 0);
    virtual bool Prefetch(const Key &key, double *wait_time = NULL); // Prefetch の宣言を追加
    virtual bool Unfetch(const Key &key);                            // Unfetch の宣言を追加

    // Initmutex() はコンストラクタで初期化されるため不要
    // void Initmutex();

private:
    std::map<Key, Value *> objects_;
    pthread_mutex_t mutex_;
};

#endif // _DB_BACKEND_SIMPLE_STORAGE_H_