#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <iostream>
#include <memory>

#include "applications/microbenchmark.h"
#include "applications/tpcc.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "backend/storage.h"
#include "backend/storage_manager.h"
#include "backend/fetching_storage.h" // Use FetchingStorage
#include "scheduler/deterministic_scheduler.h"
#include "sequencer/sequencer.h"
#include "proto/tpcc_args.pb.h"
#include "backend/txn_proto_ext.h" // TxnProtoExtの定義のためにインクルード

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
    MClient(Configuration *config, int percent_mp)
        : microbenchmark(config->all_nodes.size(), HOT),
          config_(config),
          percent_mp_(percent_mp) {}

    virtual ~MClient() {}

    virtual void GetTxn(TxnProtoExt **txn, int txn_id)
    {
        if (config_->all_nodes.size() > 1 && rand() % 100 < percent_mp_)
        {
            int other;
            do
            {
                other = rand() % config_->all_nodes.size();
            } while (other == config_->this_node_id);
            *txn = microbenchmark.MicroTxnMP(txn_id, config_->this_node_id, other);
        }
        else
        {
            *txn = microbenchmark.MicroTxnSP(txn_id, config_->this_node_id);
        }
    }

private:
    Microbenchmark microbenchmark;
    Configuration *config_;
    int percent_mp_;
};

int main(int argc, char **argv)
{
    if (argc < 4)
    {
        fprintf(stderr, "Usage: %s <node-id> <m[icro]|t[pcc]> <percent_mp>\n", argv[0]);
        exit(1);
    }

    signal(SIGINT, stop);
    signal(SIGTERM, stop);

    Configuration config(StringToInt(argv[1]), "deploy-run.conf");
    ConnectionMultiplexer multiplexer(&config);

    // ConnectionMultiplexerのスレッドが起動し、接続要求を処理する準備ができるまで待機
    // NewConnection()がnullptrを返す間はスピンし続ける
    Connection *sequencer_conn = nullptr;
    while (sequencer_conn == nullptr)
    {
        sequencer_conn = multiplexer.NewConnection("sequencer");
        if (sequencer_conn == nullptr)
        {
            Spin(0.001); // 短くスピンしてCPUを解放
        }
    }

    Connection *scheduler_conn = nullptr;
    while (scheduler_conn == nullptr)
    {
        scheduler_conn = multiplexer.NewConnection("scheduler_");
        if (scheduler_conn == nullptr)
        {
            Spin(0.001); // 短くスピンしてCPUを解放
        }
    }

    Storage *backend = FetchingStorage::BuildStorage();

    std::unique_ptr<Application> app;
    std::unique_ptr<Client> client;

    if (argv[2][0] == 'm')
    {
        app = std::make_unique<Microbenchmark>(config.all_nodes.size(), HOT);
        client = std::make_unique<MClient>(&config, atoi(argv[3]));
    }
    else
    {
        app = std::make_unique<TPCC_OCC>();
        client = std::make_unique<TClient>(&config, atoi(argv[3]));
    }

    app->InitializeStorage(backend, &config);

    auto sequencer = std::make_unique<Sequencer>(
        &config,
        sequencer_conn, // 確立した接続を渡す
        client.get(),
        backend);

    auto scheduler = std::make_unique<DeterministicScheduler>(
        &config,
        scheduler_conn, // 確立した接続を渡す
        backend,
        app.get());

    printf("System started. Spinning for 180 seconds...\n");
    Spin(180);

    printf("System shutting down.\n");
    return 0;
}