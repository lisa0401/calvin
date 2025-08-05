#ifndef _DB_APPLICATIONS_TPCC_H_
#define _DB_APPLICATIONS_TPCC_H_

#include <memory>
#include <string>
#include "applications/application.h"
#include "common/types.h"

// Forward declarations
class Configuration;
class Storage;
class StorageManager;
class TxnProto;
class TxnProtoExt;
class Warehouse;
class District;
class Customer;
class Item;
class Stock;

#define WAREHOUSES_PER_NODE 1
#define DISTRICTS_PER_WAREHOUSE 2
#define CUSTOMERS_PER_DISTRICT 100
#define NUMBER_OF_ITEMS 1000
#define ORDERS_PER_DISTRICT 100

// #define WAREHOUSES_PER_NODE 12
// #define DISTRICTS_PER_WAREHOUSE 10
// #define DISTRICTS_PER_NODE (WAREHOUSES_PER_NODE * DISTRICTS_PER_WAREHOUSE)
// #define CUSTOMERS_PER_DISTRICT 3000
// #define CUSTOMERS_PER_NODE (DISTRICTS_PER_NODE * CUSTOMERS_PER_DISTRICT)
// #define NUMBER_OF_ITEMS 100000
// #define ORDERS_PER_DISTRICT 100

enum TPCCTransactionType
{
    NEW_ORDER = 0,
    PAYMENT,
    DELIVERY,
    ORDER_STATUS,
    STOCK_LEVEL
};

class TPCC : public Application
{
public:
    virtual ~TPCC() {}

    virtual TxnProtoExt *NewTxn(int64_t txn_id, int txn_type, std::string args,
                                Configuration *config) const = 0;

    virtual int Execute(TxnProtoExt *txn, StorageManager *storage) const = 0;

    virtual void InitializeStorage(Storage *storage,
                                   Configuration *config) const = 0;
};

class TPCC_OCC : public TPCC
{
public:
    virtual ~TPCC_OCC() {}

    virtual TxnProtoExt *NewTxn(int64_t txn_id, int txn_type, std::string args,
                                Configuration *config) const;

    virtual int Execute(TxnProtoExt *txn, StorageManager *storage) const;

    virtual void InitializeStorage(Storage *storage,
                                   Configuration *config) const;

private:
    void NewOrderTransaction(TxnProtoExt *txn, StorageManager *storage) const;
    void PaymentTransaction(TxnProtoExt *txn, StorageManager *storage) const;
    void OrderStatusTransaction(TxnProtoExt *txn, StorageManager *storage) const;
    void DeliveryTransaction(TxnProtoExt *txn, StorageManager *storage) const;
    void StockLevelTransaction(TxnProtoExt *txn, StorageManager *storage) const;

    std::unique_ptr<Warehouse> CreateWarehouse(Key warehouse_key) const;
    std::unique_ptr<District> CreateDistrict(Key district_key,
                                             Key warehouse_key) const;
    std::unique_ptr<Customer> CreateCustomer(Key customer_key, Key district_key,
                                             Key warehouse_key) const;
    std::unique_ptr<Stock> CreateStock(Key item_key, Key warehouse_key) const;
    std::unique_ptr<Item> CreateItem(Key item_key, bool original) const;
};

#endif
