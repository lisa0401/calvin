#include "applications/tpcc.h"

#include <memory>
#include <string>
#include <iostream>
#include <set>

#include "backend/storage_manager.h"
#include "common/utils.h"
#include "common/configuration.h"
#include "proto/tpcc.pb.h"
#include "proto/tpcc_args.pb.h"
#include "backend/txn_proto_ext.h"
#include "backend/storage.h"

using std::string;

// TPC-Cのトランザクションタイプに応じて、対応する処理関数を呼び出す。
int TPCC_OCC::Execute(TxnProtoExt *txn, StorageManager *storage) const
{
    switch (txn->txn_type())
    {
    case NEW_ORDER:
        NewOrderTransaction(txn, storage);
        break;
    case PAYMENT:
        PaymentTransaction(txn, storage);
        break;
    case DELIVERY:
        DeliveryTransaction(txn, storage);
        break;
    case ORDER_STATUS:
        OrderStatusTransaction(txn, storage);
        break;
    case STOCK_LEVEL:
        StockLevelTransaction(txn, storage);
        break;
    default:
        // 未知のトランザクションタイプ
        break;
    }
    return 0; // SUCCESS
}

TxnProtoExt *TPCC_OCC::NewTxn(int64 txn_id, int txn_type, string args, Configuration *config) const
{
    auto *txn = new TxnProtoExt();
    txn->set_txn_id(txn_id);
    txn->set_txn_type(txn_type);
    txn->set_arg(args);
    // writeトランザクションのデフォルトとしてfalseに設定
    txn->set_is_read_only(false);

    int w_id = rand() % WAREHOUSES_PER_NODE;
    int d_id = rand() % DISTRICTS_PER_WAREHOUSE;
    int c_id = rand() % CUSTOMERS_PER_DISTRICT;
    int o_id = rand() % ORDERS_PER_DISTRICT;

    char w_key_buf[128], d_key_buf[128], c_key_buf[128];
    snprintf(w_key_buf, sizeof(w_key_buf), "w%d", w_id);
    snprintf(d_key_buf, sizeof(d_key_buf), "w%dd%d", w_id, d_id);
    snprintf(c_key_buf, sizeof(c_key_buf), "w%dd%dc%d", w_id, d_id, c_id);

    switch (txn_type)
    {
    case NEW_ORDER:
    {
        txn->add_read_set(w_key_buf);
        txn->add_read_set(c_key_buf);
        txn->add_read_write_set(d_key_buf); // next_order_idを更新するため

        char o_key_buf[128], no_key_buf[128];
        snprintf(o_key_buf, sizeof(o_key_buf), "%so%d", d_key_buf, o_id);
        snprintf(no_key_buf, sizeof(no_key_buf), "%sno%d", d_key_buf, o_id);
        txn->add_write_set(o_key_buf);
        txn->add_write_set(no_key_buf);

        for (int i = 0; i < 10; ++i)
        { // 10 order lines per order
            int item_id = rand() % NUMBER_OF_ITEMS;
            char s_key_buf[128], ol_key_buf[128];
            snprintf(s_key_buf, sizeof(s_key_buf), "w%dsi%d", w_id, item_id);
            snprintf(ol_key_buf, sizeof(ol_key_buf), "%sol%d", o_key_buf, i);
            txn->add_read_write_set(s_key_buf); // stock quantityを更新
            txn->add_write_set(ol_key_buf);
        }
        // New-Orderはwriteトランザクションなのでis_read_onlyはfalseのまま
        break;
    }
    case PAYMENT:
    {
        txn->add_read_write_set(w_key_buf);
        txn->add_read_write_set(d_key_buf);
        txn->add_read_write_set(c_key_buf);
        char h_key_buf[128];
        snprintf(h_key_buf, sizeof(h_key_buf), "h%ld", txn_id); // unique history key
        txn->add_write_set(h_key_buf);
        // Paymentはwriteトランザクションなのでis_read_onlyはfalseのまま
        break;
    }
    case DELIVERY:
    {
        for (int d = 0; d < DISTRICTS_PER_WAREHOUSE; ++d)
        {
            char current_d_key[128];
            snprintf(current_d_key, sizeof(current_d_key), "w%dd%d", w_id, d);
            int oldest_o_id = d;
            char o_key_buf[128], no_key_buf[128], c_key_for_o[128];
            snprintf(o_key_buf, sizeof(o_key_buf), "%so%d", current_d_key, oldest_o_id);
            snprintf(no_key_buf, sizeof(no_key_buf), "%sno%d", current_d_key, oldest_o_id);
            snprintf(c_key_for_o, sizeof(c_key_for_o), "%sc%d", current_d_key, oldest_o_id);

            txn->add_read_write_set(o_key_buf);
            txn->add_write_set(no_key_buf);
            txn->add_read_write_set(c_key_for_o);

            for (int ol = 0; ol < 10; ++ol)
            {
                char ol_key_buf[128];
                snprintf(ol_key_buf, sizeof(ol_key_buf), "%sol%d", o_key_buf, ol);
                txn->add_read_write_set(ol_key_buf);
            }
        }
        // Deliveryはwriteトランザクションなのでis_read_onlyはfalseのまま
        break;
    }
    case ORDER_STATUS:
    {
        txn->add_read_set(c_key_buf);
        char o_key_buf[128];
        snprintf(o_key_buf, sizeof(o_key_buf), "%so%d", d_key_buf, o_id);
        txn->add_read_set(o_key_buf);
        for (int i = 0; i < 10; ++i)
        {
            char ol_key_buf[128];
            snprintf(ol_key_buf, sizeof(ol_key_buf), "%sol%d", o_key_buf, i);
            txn->add_read_set(ol_key_buf);
        }
        // Order-Statusはread-onlyトランザクションなのでフラグをtrueに設定
        txn->set_is_read_only(true);
        break;
    }
    case STOCK_LEVEL:
    {
        txn->add_read_set(d_key_buf);
        for (int i = 0; i < 20; ++i)
        {
            int order_id = (o_id - i + ORDERS_PER_DISTRICT) % ORDERS_PER_DISTRICT;
            char o_key_buf[128];
            snprintf(o_key_buf, sizeof(o_key_buf), "%so%d", d_key_buf, order_id);
            txn->add_read_set(o_key_buf);
        }
        // Stock-Levelはread-onlyトランザクションなのでフラグをtrueに設定
        txn->set_is_read_only(true);
        break;
    }
    }

    return txn;
}

void TPCC_OCC::NewOrderTransaction(TxnProtoExt *txn, StorageManager *storage) const
{
    auto warehouse_value = std::unique_ptr<Value>(storage->ReadObject(txn->read_set(0)));
    auto customer_value = std::unique_ptr<Value>(storage->ReadObject(txn->read_set(1)));
    if (!warehouse_value || !customer_value)
        return;

    auto district_key = txn->read_write_set(0);
    auto district_value = std::unique_ptr<Value>(storage->ReadObject(district_key));
    if (!district_value)
        return;

    District district;
    district.ParseFromString(district_value->data);
    district.set_next_order_id(district.next_order_id() + 1);
    district.SerializeToString(&district_value->data);                         // <- 修正
    storage->PutObject(district_key, district_value.release(), txn->txn_id()); // <- 修正

    string o_key = txn->write_set(0);
    auto order_value = std::make_unique<Value>();
    Order order;
    order.set_id(o_key);
    order.SerializeToString(&order_value->data);                     // <- 修正
    storage->PutObject(o_key, order_value.release(), txn->txn_id()); // <- 修正

    string no_key = txn->write_set(1);
    auto new_order_value = std::make_unique<Value>();
    NewOrder new_order;
    new_order.set_id(no_key);
    new_order.SerializeToString(&new_order_value->data);                  // <- 修正
    storage->PutObject(no_key, new_order_value.release(), txn->txn_id()); // <- 修正

    for (int i = 0; i < 10; ++i)
    {
        string stock_key = txn->read_write_set(i + 1);
        auto stock_value = std::unique_ptr<Value>(storage->ReadObject(stock_key));
        if (!stock_value)
            continue;

        Stock stock;
        stock.ParseFromString(stock_value->data);
        stock.set_quantity(stock.quantity() > 10 ? stock.quantity() - 1 : stock.quantity() + 90);
        stock.SerializeToString(&stock_value->data);                         // <- 修正
        storage->PutObject(stock_key, stock_value.release(), txn->txn_id()); // <- 修正

        string ol_key = txn->write_set(i + 2);
        auto ol_value = std::make_unique<Value>();
        OrderLine ol;
        ol.set_order_id(o_key);
        ol.SerializeToString(&ol_value->data);                         // <- 修正
        storage->PutObject(ol_key, ol_value.release(), txn->txn_id()); // <- 修正
    }
}

void TPCC_OCC::PaymentTransaction(TxnProtoExt *txn, StorageManager *storage) const
{
    auto warehouse_key = txn->read_write_set(0);
    auto district_key = txn->read_write_set(1);
    auto customer_key = txn->read_write_set(2);

    auto warehouse_value = std::unique_ptr<Value>(storage->ReadObject(warehouse_key));
    auto district_value = std::unique_ptr<Value>(storage->ReadObject(district_key));
    auto customer_value = std::unique_ptr<Value>(storage->ReadObject(customer_key));
    if (!warehouse_value || !district_value || !customer_value)
        return;

    int payment_amount = 100;

    Warehouse warehouse;
    warehouse.ParseFromString(warehouse_value->data);
    warehouse.set_year_to_date(warehouse.year_to_date() + payment_amount);
    warehouse.SerializeToString(&warehouse_value->data);                         // <- 修正
    storage->PutObject(warehouse_key, warehouse_value.release(), txn->txn_id()); // <- 修正

    District district;
    district.ParseFromString(district_value->data);
    district.set_year_to_date(district.year_to_date() + payment_amount);
    district.SerializeToString(&district_value->data);                         // <- 修正
    storage->PutObject(district_key, district_value.release(), txn->txn_id()); // <- 修正

    Customer customer;
    customer.ParseFromString(customer_value->data);
    customer.set_balance(customer.balance() - payment_amount);
    customer.set_payment_count(customer.payment_count() + 1);
    customer.SerializeToString(&customer_value->data);                         // <- 修正
    storage->PutObject(customer_key, customer_value.release(), txn->txn_id()); // <- 修正

    auto history_key = txn->write_set(0);
    auto history_value = std::make_unique<Value>();
    History history;
    history.set_customer_id(customer_key);
    history.set_amount(payment_amount);
    history.SerializeToString(&history_value->data);                         // <- 修正
    storage->PutObject(history_key, history_value.release(), txn->txn_id()); // <- 修正
}

void TPCC_OCC::DeliveryTransaction(TxnProtoExt *txn, StorageManager *storage) const
{
    // Implementation
}

void TPCC_OCC::OrderStatusTransaction(TxnProtoExt *txn, StorageManager *storage) const
{
    // Read-only, no implementation needed for this logic
}

void TPCC_OCC::StockLevelTransaction(TxnProtoExt *txn, StorageManager *storage) const
{
    // Read-only, no implementation needed for this logic
}

std::unique_ptr<Warehouse> TPCC_OCC::CreateWarehouse(Key warehouse_key) const
{
    auto warehouse = std::make_unique<Warehouse>();
    warehouse->set_id(warehouse_key);
    warehouse->set_name(RandomString(10));
    warehouse->set_street_1(RandomString(20));
    warehouse->set_street_2(RandomString(20));
    warehouse->set_city(RandomString(20));
    warehouse->set_state(RandomString(2));
    warehouse->set_zip(RandomString(9));
    warehouse->set_tax(0.05);
    warehouse->set_year_to_date(0.0);
    return warehouse;
}

std::unique_ptr<District> TPCC_OCC::CreateDistrict(Key district_key, Key warehouse_key) const
{
    auto district = std::make_unique<District>();
    district->set_id(district_key);
    district->set_warehouse_id(warehouse_key);
    district->set_name(RandomString(10));
    district->set_street_1(RandomString(20));
    district->set_street_2(RandomString(20));
    district->set_city(RandomString(20));
    district->set_state(RandomString(2));
    district->set_zip(RandomString(9));
    district->set_tax(0.05);
    district->set_year_to_date(0.0);
    district->set_next_order_id(ORDERS_PER_DISTRICT);
    return district;
}

std::unique_ptr<Customer> TPCC_OCC::CreateCustomer(Key customer_key, Key district_key, Key warehouse_key) const
{
    auto customer = std::make_unique<Customer>();
    customer->set_id(customer_key);
    customer->set_district_id(district_key);
    customer->set_warehouse_id(warehouse_key);
    customer->set_first(RandomString(20));
    customer->set_middle("OE");
    customer->set_last(RandomString(16));
    customer->set_street_1(RandomString(20));
    customer->set_street_2(RandomString(20));
    customer->set_city(RandomString(20));
    customer->set_state(RandomString(2));
    customer->set_zip(RandomString(9));
    customer->set_since(0);
    customer->set_credit("GC");
    customer->set_credit_limit(50000);
    customer->set_discount(0.15);
    customer->set_balance(0);
    customer->set_year_to_date_payment(0);
    customer->set_payment_count(0);
    customer->set_delivery_count(0);
    customer->set_data(RandomString(500));
    return customer;
}

std::unique_ptr<Item> TPCC_OCC::CreateItem(Key item_key, bool original) const
{
    // (この関数は変更なし)
    auto item = std::make_unique<Item>();
    item->set_id(item_key);
    item->set_name(RandomString(24));
    item->set_price(rand() % 100 + 1);
    string data = RandomString(50);
    if (original)
    {
        data.replace(8, 8, "ORIGINAL");
    }
    item->set_data(data);
    return item;
}

std::unique_ptr<Stock> TPCC_OCC::CreateStock(Key item_key, Key warehouse_key) const
{
    // (この関数は変更なし)
    auto stock = std::make_unique<Stock>();
    char stock_key[128];
    snprintf(stock_key, sizeof(stock_key), "%ssi%s", warehouse_key.c_str(), item_key.substr(1).c_str());
    stock->set_id(stock_key);
    stock->set_warehouse_id(warehouse_key);
    stock->set_item_id(item_key);
    stock->set_quantity(rand() % 91 + 10);
    stock->set_year_to_date(0);
    stock->set_order_count(0);
    stock->set_remote_count(0);
    stock->set_data(RandomString(50));
    return stock;
}

// InitializeStorageではStorageManagerではなくStorageを使うため、PutObjectは2引数で正しい
void TPCC_OCC::InitializeStorage(Storage *storage, Configuration *config) const
{
    if (config->this_node_id == 0)
    {
        for (int i = 0; i < NUMBER_OF_ITEMS; ++i)
        {
            char i_key[128];
            snprintf(i_key, sizeof(i_key), "i%d", i);
            auto item = CreateItem(i_key, (rand() % 10 == 0));
            auto item_value = std::make_unique<Value>();
            item->SerializeToString(&item_value->data); // <- 修正
            storage->PutObject(i_key, item_value.release());
        }
    }

    for (int w = 0; w < WAREHOUSES_PER_NODE; ++w)
    {
        char w_key[128];
        snprintf(w_key, sizeof(w_key), "w%d", w);

        auto warehouse = CreateWarehouse(w_key);
        auto warehouse_value = std::make_unique<Value>();
        warehouse->SerializeToString(&warehouse_value->data); // <- 修正
        storage->PutObject(w_key, warehouse_value.release());

        for (int i = 0; i < NUMBER_OF_ITEMS; ++i)
        {
            char i_key[128];
            snprintf(i_key, sizeof(i_key), "i%d", i);
            auto stock = CreateStock(i_key, w_key);
            auto stock_value = std::make_unique<Value>();
            stock->SerializeToString(&stock_value->data); // <- 修正
            storage->PutObject(stock->id(), stock_value.release());
        }

        for (int d = 0; d < DISTRICTS_PER_WAREHOUSE; ++d)
        {
            char d_key[128];
            snprintf(d_key, sizeof(d_key), "%sd%d", w_key, d);

            auto district = CreateDistrict(d_key, w_key);
            auto district_value = std::make_unique<Value>();
            district->SerializeToString(&district_value->data); // <- 修正
            storage->PutObject(d_key, district_value.release());

            for (int c = 0; c < CUSTOMERS_PER_DISTRICT; ++c)
            {
                char c_key[128];
                snprintf(c_key, sizeof(c_key), "%sc%d", d_key, c);

                auto customer = CreateCustomer(c_key, d_key, w_key);
                auto customer_value = std::make_unique<Value>();
                customer->SerializeToString(&customer_value->data); // <- 修正
                storage->PutObject(c_key, customer_value.release());

                for (int o = 0; o < ORDERS_PER_DISTRICT; ++o)
                {
                    char o_key[128];
                    snprintf(o_key, sizeof(o_key), "%so%d", d_key, o);

                    auto order = std::make_unique<Order>();
                    order->set_id(o_key);
                    order->set_customer_id(c_key);
                    order->set_carrier_id(rand() % 15 + 1);
                    order->set_order_line_count(10);
                    order->set_all_items_local(true);

                    auto order_value = std::make_unique<Value>();
                    order->SerializeToString(&order_value->data); // <- 修正
                    storage->PutObject(o_key, order_value.release());

                    if (o >= ORDERS_PER_DISTRICT * 2 / 3)
                    {
                        char no_key[128];
                        snprintf(no_key, sizeof(no_key), "%sno%d", d_key, o);
                        auto new_order = std::make_unique<NewOrder>();
                        new_order->set_id(no_key);
                        auto new_order_value = std::make_unique<Value>();
                        new_order->SerializeToString(&new_order_value->data); // <- 修正
                        storage->PutObject(no_key, new_order_value.release());
                    }

                    for (int ol = 0; ol < 10; ++ol)
                    {
                        char ol_key[128];
                        snprintf(ol_key, sizeof(ol_key), "%sol%d", o_key, ol);
                        auto order_line = std::make_unique<OrderLine>();
                        order_line->set_order_id(o_key);
                        char i_key[128];
                        snprintf(i_key, sizeof(i_key), "i%d", rand() % NUMBER_OF_ITEMS);
                        order_line->set_item_id(i_key);
                        order_line->set_supply_warehouse_id(w_key);
                        order_line->set_quantity(5);
                        order_line->set_amount((o < ORDERS_PER_DISTRICT * 2 / 3) ? (rand() % 1000) / 100.0 : 0);
                        if (o < ORDERS_PER_DISTRICT * 2 / 3)
                        {
                            order_line->set_delivery_date(GetTime());
                        }

                        auto ol_value = std::make_unique<Value>();
                        order_line->SerializeToString(&ol_value->data); // <- 修正
                        storage->PutObject(ol_key, ol_value.release());
                    }
                }
            }
        }
    }
    std::cout << "Finished initializing storage." << std::endl;
}