#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "applications/tpcc.h"
#include "applications/microbenchmark.h"
#include "applications/ycsb.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/definitions.hh" // ★★★ FIX: Include definitions header ★★★
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "backend/fetching_storage.h"
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"
#include "backend/txn_proto_ext.h"

using std::string;

// Signal handler for graceful termination
void stop(int sig)
{
    exit(sig);
}

// Client implementation for TPC-C
class TClient : public Client
{
public:
    TClient(Configuration *config, int percent_mp)
        : config_(config), percent_mp_(percent_mp) {}
    virtual ~TClient() {}

    virtual void GetTxn(TxnProtoExt **txn, int txn_id)
    {
        TPCC_OCC tpcc_app;
        string args_string;
        TPCCArgs args;
        args.set_system_time(GetTime());
        args.set_multipartition(rand() % 100 < percent_mp_);
        args.SerializeToString(&args_string);

        int r = rand() % 100;
        int txn_type;
        if (r < 45)
            txn_type = NEW_ORDER;
        else if (r < 88)
            txn_type = PAYMENT;
        else if (r < 92)
            txn_type = ORDER_STATUS;
        else if (r < 96)
            txn_type = DELIVERY;
        else
            txn_type = STOCK_LEVEL;

        *txn = tpcc_app.NewTxn(txn_id, txn_type, args_string, config_);
    }

private:
    Configuration *config_;
    int percent_mp_;
};

// Client implementation for Microbenchmark
class MClient : public Client
{
public:
    MClient(Configuration *config, int percent_mp, Microbenchmark *app)
        : microbenchmark_app_(app),
          config_(config),
          percent_mp_(percent_mp) {}
    virtual ~MClient() {}

    virtual void GetTxn(TxnProtoExt **txn, int txn_id)
    {
        *txn = microbenchmark_app_->NewTxn(txn_id, 0, "", config_);
    }

private:
    Microbenchmark *microbenchmark_app_;
    Configuration *config_;
    int percent_mp_;
};

// Client implementation for YCSB
class YClient : public Client
{
public:
    YClient(Configuration *config, YCSB *app) : ycsb_app_(app), config_(config) {}
    virtual ~YClient() {}

    virtual void GetTxn(TxnProtoExt **txn, int txn_id)
    {
        *txn = ycsb_app_->NewTxn(txn_id, 0, "", config_);
    }

private:
    YCSB *ycsb_app_;
    Configuration *config_;
};

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]|y[csb]> <percent_mp>\n", argv[0]);
        exit(1);
    }

    signal(SIGINT, stop);
    signal(SIGTERM, stop);

    Configuration config(StringToInt(argv[1]), "deploy-run.conf");
    ConnectionMultiplexer multiplexer(&config);

    Connection *sequencer_conn = nullptr;
    while (sequencer_conn == nullptr)
    {
        sequencer_conn = multiplexer.NewConnection("sequencer");
        if (sequencer_conn == nullptr)
        {
            Spin(0.001);
        }
    }

    Connection *scheduler_conn = nullptr;
    while (scheduler_conn == nullptr)
    {
        scheduler_conn = multiplexer.NewConnection("scheduler_");
        if (scheduler_conn == nullptr)
        {
            Spin(0.001);
        }
    }

    Storage *backend = FetchingStorage::BuildStorage();

    std::unique_ptr<Application> app;
    std::unique_ptr<Client> client;

    if (argv[2][0] == 'm')
    {
        auto mb_app = std::make_unique<Microbenchmark>(config.all_nodes.size(), HOT_RECORDS);
        client = std::make_unique<MClient>(&config, atoi(argv[3]), mb_app.get());
        app = std::move(mb_app);
    }
    else if (argv[2][0] == 't')
    {
        app = std::make_unique<TPCC_OCC>();
        client = std::make_unique<TClient>(&config, atoi(argv[3]));
    }
    else if (argv[2][0] == 'y')
    {
        auto ycsb_app = std::make_unique<YCSB>();
        client = std::make_unique<YClient>(&config, ycsb_app.get());
        app = std::move(ycsb_app);
    }
    else
    {
        fprintf(stderr, "Invalid application type: %s\n", argv[2]);
        exit(1);
    }

    app->InitializeStorage(backend, &config);

    auto sequencer = std::make_unique<Sequencer>(
        &config,
        sequencer_conn,
        client.get(),
        backend);

    auto scheduler = std::make_unique<DeterministicScheduler>(
        &config,
        scheduler_conn,
        backend,
        app.get());

    printf("System started. Spinning for 180 seconds...\n");
    Spin(180);

    printf("System shutting down.\n");
    return 0;
}
