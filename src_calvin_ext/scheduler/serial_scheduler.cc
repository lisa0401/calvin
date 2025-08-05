// scheduler/serial_scheduler.cc
#include "scheduler/serial_scheduler.h"

#include <iostream>

#include "applications/application.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "backend/storage_manager.h"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "backend/txn_proto_ext.h"

SerialScheduler::SerialScheduler(Configuration *conf,
                                 Connection *connection,
                                 Storage *storage,
                                 bool checkpointing)
    : configuration_(conf),
      connection_(connection),
      storage_(storage),
      checkpointing_(checkpointing) {}

SerialScheduler::~SerialScheduler() {}

void SerialScheduler::Run(const Application &application)
{
    MessageProto message;
    Connection *manager_connection =
        connection_->multiplexer()->NewConnection("manager_connection");

    int txns = 0;
    double time = GetTime();
    double start_time = time;
    while (true)
    {
        if (connection_->GetMessage(&message))
        {
            // Execute all txns in batch.
            for (int i = 0; i < message.data_size(); i++)
            {
                // TxnProtoExtとして新しいトランザクションを生成
                TxnProtoExt *txn = new TxnProtoExt();
                txn->ParseFromString(message.data(i));

                // Link txn-specific channel to manager_connection.
                manager_connection->LinkChannel(IntToString(txn->txn_id()));

                // Create manager. The manager will handle fetching remote data.
                StorageManager *manager = new StorageManager(configuration_, manager_connection,
                                                             storage_, txn);

                // Wait until all remote reads are complete.
                while (!manager->ReadyToExecute())
                {
                    MessageProto result_message;
                    if (manager_connection->GetMessage(&result_message))
                        manager->HandleReadResult(result_message);
                }

                // Execute the transaction using the unified OCC logic in TxnProtoExt.
                txn->Execute(manager, &application);

                // Clean up the mess.
                delete manager;
                manager_connection->UnlinkChannel(IntToString(txn->txn_id()));
                delete txn;

                // Report throughput.
                txns++;
            }
        }

        // Report throughput (once per second).
        if (GetTime() > time + 1)
        {
            std::cout << "Executed " << txns << " txns/sec\n"
                      << std::flush;
            time = GetTime();
            txns = 0;
        }

        // Run for at most one minute.
        if (GetTime() > start_time + 60)
            exit(0);
    }

    delete manager_connection;
}
