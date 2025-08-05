#ifndef _DB_APPLICATIONS_APPLICATION_H_
#define _DB_APPLICATIONS_APPLICATION_H_

#include <string>
#include <cstdint> // For int64_t

#include "backend/txn_proto_ext.h"

class Configuration;
class Storage;
class StorageManager;

class Application
{
public:
    virtual ~Application() {}

    // Use standard int64_t for portability.
    virtual TxnProtoExt *NewTxn(int64_t txn_id, int txn_type, std::string args,
                                Configuration *config) const = 0;

    // Use TxnProtoExt* for consistency across the application interface.
    // This ensures derived classes correctly implement the virtual function.
    virtual int Execute(TxnProtoExt *txn, StorageManager *storage) const = 0;

    virtual void InitializeStorage(Storage *storage,
                                   Configuration *config) const = 0;
};

#endif // _DB_APPLICATIONS_APPLICATION_H_
