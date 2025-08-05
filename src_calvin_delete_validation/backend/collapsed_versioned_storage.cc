// backend/collapsed_versioned_storage.cc
#include "backend/collapsed_versioned_storage.h"

#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <memory> // std::unique_ptr を使用するために追加

using std::string;

// コンストラクタ: ミューテックスを初期化
CollapsedVersionedStorage::CollapsedVersionedStorage() : stable_(0)
{
    // TPCCHACK はヘッダーファイルでコメントアウトされているか、削除されているはずです。
    // ここでは汎用的なストレージとして動作させます。
    pthread_mutex_init(&mutex_, NULL); // ミューテックスを初期化
}

// デストラクタ: ミューテックスを破棄し、DataNodeとValueのメモリを解放
CollapsedVersionedStorage::~CollapsedVersionedStorage()
{
    pthread_mutex_destroy(&mutex_); // ミューテックスを破棄

    // objects_ 内の全ての DataNode と Value を解放
    for (auto const &pair : objects_)
    {
        DataNode *current = pair.second;
        while (current)
        {
            DataNode *next = current->next;
            // DataNode の value が nullptr でない場合のみ解放（論理削除されたものはnullptr）
            if (current->value)
            {
                delete current->value; // Value* を解放
            }
            delete current; // DataNode を解放
            current = next;
        }
    }
    objects_.clear();
}

// ReadObject: 指定されたキーとトランザクションIDに基づいて値を読み取る
// Returns nullptr if the latest visible version is a tombstone (logically deleted).
Value *CollapsedVersionedStorage::ReadObject(const Key &key, int64 txn_id)
{
    pthread_mutex_lock(&mutex_); // ロック開始

    // キーが存在しない場合は nullptr を返す
    if (objects_.count(key) == 0)
    {
        pthread_mutex_unlock(&mutex_); // ロック解除
        return nullptr;
    }

    // バージョンリストを検索 (最新から古い順)
    for (DataNode *list = objects_[key]; list; list = list->next)
    {
        // トランザクションID以下の最新バージョンを見つける
        if (list->txn_id <= txn_id)
        {
            if (list->value) // Value が存在する場合 (削除マーカーでない場合)
            {
                Value *result = new Value(*list->value); // コピーをヒープに作成し、所有権を呼び出し元に移譲
                pthread_mutex_unlock(&mutex_);           // ロック解除
                return result;
            }
            else
            {
                // 論理的に削除されたバージョン (tombstone) が見つかった
                pthread_mutex_unlock(&mutex_); // ロック解除
                return nullptr;                // オブジェクトは論理的に削除済み
            }
        }
    }

    // 適切なバージョンが見つからない場合 (全てのバージョンが現在のtxn_idよりも新しい)
    pthread_mutex_unlock(&mutex_); // ロック解除
    return nullptr;
}

// PutObject: 新しいバージョンを挿入する
// 新しいバージョンは常にリストの先頭に追加されます。
bool CollapsedVersionedStorage::PutObject(const Key &key, Value *value, int64 txn_id)
{
    pthread_mutex_lock(&mutex_); // ロック開始

    // 新しいバージョンをリストに挿入するDataNodeを作成
    DataNode *item = new DataNode();
    item->txn_id = txn_id;
    item->value = value; // Value* の所有権を DataNode に移譲
    item->next = nullptr;

    // Keyが既に存在する場合、既存のリストの先頭に新しいバージョンを追加
    if (objects_.count(key) != 0)
    {
        item->next = objects_[key]; // 既存のリストの先頭を新しいノードの次にする
    }
    objects_[key] = item; // 新しいノードをマップの先頭に設定

    pthread_mutex_unlock(&mutex_); // ロック解除
    return true;
}

// DeleteObject: 論理的な削除を表す「tombstone」バージョンを挿入する
// これにより、オブジェクトは将来のトランザクションに対して論理的に削除済みとしてマークされます。
bool CollapsedVersionedStorage::DeleteObject(const Key &key, int64 txn_id)
{
    pthread_mutex_lock(&mutex_); // ロック開始

    // 論理的な削除を表す DataNode (tombstone) を作成
    DataNode *item = new DataNode();
    item->txn_id = txn_id;
    item->value = nullptr; // Value を NULL に設定して論理的な削除を示す
    item->next = nullptr;

    // tombstone をキーのリストの先頭に追加
    if (objects_.count(key) != 0)
    {
        item->next = objects_[key];
    }
    objects_[key] = item;

    pthread_mutex_unlock(&mutex_); // ロック解除
    return true;
}

// Checkpoint: チェックポイント作成を非同期で開始する
int CollapsedVersionedStorage::Checkpoint()
{
    // Checkpoint処理は別スレッドで行われるため、
    // CaptureCheckpoint内でロックを適切に管理する必要があります。
    pthread_t checkpointing_daemon;
    int thread_status = pthread_create(&checkpointing_daemon, nullptr, &RunCheckpointer, this);
    return thread_status;
}

// CaptureCheckpoint: チェックポイントをファイルに書き込む
void CollapsedVersionedStorage::CaptureCheckpoint()
{
    fprintf(stdout, "Beginning checkpoint capture...\n");

    char log_name[200];
    snprintf(log_name, sizeof(log_name), "%s/%ld.checkpoint", CHKPNTDIR, stable_);
    FILE *checkpoint = fopen(log_name, "w");

    if (!checkpoint)
    {
        std::cerr << "Failed to open checkpoint file: " << log_name << std::endl;
        return;
    }

    pthread_mutex_lock(&mutex_); // チェックポイント中はマップをロック

    for (auto const &pair : objects_)
    {
        const Key &key = pair.first;
        // ReadObjectは内部でロックを取得するので、ここではロックを保持したまま呼び出さない。
        // 代わりに、DataNodeリストを直接走査してstable_バージョンを見つける。
        DataNode *current_node = pair.second;
        Value *result_value = nullptr;
        for (; current_node; current_node = current_node->next)
        {
            if (current_node->txn_id <= stable_)
            {
                result_value = current_node->value;
                break;
            }
        }

        if (result_value != nullptr && result_value->data.length() > 0)
        {
            int key_length = key.length();
            int val_length = result_value->data.length();

            fwrite(&key_length, sizeof(int), 1, checkpoint);
            fwrite(key.c_str(), 1, key_length, checkpoint);
            fwrite(&val_length, sizeof(int), 1, checkpoint); // Corrected this line in previous iteration
            fwrite(result_value->data.c_str(), 1, val_length, checkpoint);
        }

        // チェックポイント中の古いバージョンクリーンアップロジックは削除。
        // これはGCスレッドまたは別のバックグラウンドプロセスで行われるべき。
    }

    pthread_mutex_unlock(&mutex_); // ロック解除

    fclose(checkpoint);
    fprintf(stdout, "Finished checkpointing\n");
}
