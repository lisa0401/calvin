// backend/txn_proto_ext.h
#ifndef _DB_BACKEND_TXN_PROTO_EXT_H_
#define _DB_BACKEND_TXN_PROTO_EXT_H_

#include "proto/txn.pb.h"

// Forward declarations to avoid circular dependencies.
class StorageManager;
class Application;

// TxnProtoを継承し、実行ロジックを追加した拡張クラス。
// このように継承するには、.protoファイルで 'option cc_generic_services = true;'
// などを設定するか、手動でヘッダーを調整する必要があります。
// ここでは、その設定がされていることを前提とします。
class TxnProtoExt : public TxnProto
{
public:
    // トランザクションを実行するメインメソッド。
    // 内部でRead、Execute、Validate、Writeの各フェーズを順に実行します。
    // バリデーションに成功した場合はtrue、失敗した場合はfalseを返します。
    bool Execute(StorageManager *storage, const Application *app);

private:
    // Readフェーズ：read_setとread_write_setに含まれるキーのバージョンを記録します。
    bool AddReadVersions(StorageManager *storage);

    // Validateフェーズ：記録したバージョンが変更されていないか検証します。
    bool Validate(StorageManager *storage) const;

    // Writeフェーズ：トランザクションによる変更をストレージに書き込みます。
    void ApplyWrites(StorageManager *storage);
};

#endif // _DB_BACKEND_TXN_PROTO_EXT_H_
