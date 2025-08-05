#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>
#include <algorithm> // Required for std::max
#include <vector>    // Required for std::vector

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "common/debug.hh"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#include "backend/txn_proto_ext.h"
#ifdef PAXOS
#include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::queue;
using std::set;

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
// double paxos_begin[SAMPLES];
// double paxos_end[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif

void *Sequencer::RunSequencerWriter(void *arg)
{
    reinterpret_cast<Sequencer *>(arg)->RunWriter();
    return NULL;
}

void *Sequencer::RunSequencerReader(void *arg)
{
    reinterpret_cast<Sequencer *>(arg)->RunReader();
    return NULL;
}

Sequencer::Sequencer(Configuration *conf,
                     Connection *connection,
                     Client *client,
                     Storage *storage)
    : epoch_duration_(EPOCH_DURATION),
      configuration_(conf),
      connection_(connection),
      client_(client),
      storage_(storage),
      deconstructor_invoked_(false)
{
    pthread_mutex_init(&mutex_, NULL);

    cpu_set_t cpuset;
    pthread_attr_t attr_writer;
    pthread_attr_init(&attr_writer);

    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_WRITER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);

    pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter,
                   reinterpret_cast<void *>(this));

    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_READER_CORE, &cpuset);
    pthread_attr_t attr_reader;
    pthread_attr_init(&attr_reader);
    pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);

    pthread_create(&reader_thread_, &attr_reader, RunSequencerReader,
                   reinterpret_cast<void *>(this));

    // schedulerからのTXN_RETRYメッセージを受け取るためのConnectionを初期化
    scheduler_connection_ = connection->multiplexer()->NewConnection("sequencer_retry_channel");
}

Sequencer::~Sequencer()
{
    deconstructor_invoked_ = true;
    pthread_join(writer_thread_, NULL);
    pthread_join(reader_thread_, NULL);
}

void Sequencer::FindParticipatingNodes(const TxnProtoExt &txn, set<int> *nodes)
{
    nodes->clear();
    for (int i = 0; i < txn.read_set_size(); i++)
        nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
    for (int i = 0; i < txn.write_set_size(); i++)
        nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
    for (int i = 0; i < txn.read_write_set_size(); i++)
        nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

#ifdef PREFETCHING
double PrefetchAll(Storage *storage, TxnProtoExt *txn)
{
    double max_wait_time = 0;
    double wait_time = 0;
    for (int i = 0; i < txn->read_set_size(); i++)
    {
        storage->Prefetch(txn->read_set(i), &wait_time);
        max_wait_time = std::max(max_wait_time, wait_time); // Use std::max
    }
    for (int i = 0; i < txn->read_write_set_size(); i++)
    {
        storage->Prefetch(txn->read_write_set(i), &wait_time);
        max_wait_time = std::max(max_wait_time, wait_time); // Use std::max
    }
    for (int i = 0; i < txn->write_set_size(); i++)
    {
        storage->Prefetch(txn->write_set(i), &wait_time);
        max_wait_time = std::max(max_wait_time, wait_time); // Use std::max
    }
#ifdef LATENCY_TEST
    if (txn->txn_id() % SAMPLE_RATE == 0)
        prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
    return max_wait_time;
}
#endif

void Sequencer::RunWriter()
{
    PrintCpu("RunWriter", 0);

    Spin(1);

#ifdef PAXOS
    Paxos paxos(ZOOKEEPER_CONF, false);
#endif

#ifdef PREFETCHING
    multimap<double, TxnProtoExt *> fetching_txns;
#endif

    // Synchronization loadgen start with other sequencers.
    MessageProto synchronization_message;
    synchronization_message.set_type(MessageProto::EMPTY);
    synchronization_message.set_destination_channel("sequencer");

    if (configuration_->all_nodes.size() > 1)
    {
        for (uint32_t i = 0; i < configuration_->all_nodes.size(); i++)
        {
            synchronization_message.set_destination_node(i);
            if (i != static_cast<uint32_t>(configuration_->this_node_id))
                connection_->Send(synchronization_message);
        }
        uint32_t synchronization_counter = 1;
        while (synchronization_counter < configuration_->all_nodes.size())
        {
            synchronization_message.Clear();
            if (connection_->GetMessage(&synchronization_message))
            {
                assert(synchronization_message.type() == MessageProto::EMPTY);
                synchronization_counter++;
            }
        }
    }

    std::cout << "Starting sequencer.\n"
              << std::flush;

    // Set up batch messages for each system node.
    MessageProto batch;
    batch.set_destination_channel("sequencer");
    batch.set_destination_node(-1);
    string batch_string;
    batch.set_type(MessageProto::TXN_BATCH);

    for (int batch_number = configuration_->this_node_id; !deconstructor_invoked_;
         batch_number += configuration_->all_nodes.size())
    {
        // Begin epoch.
        double epoch_start = GetTime();
        batch.set_batch_number(batch_number);
        batch.clear_data();

        // Collect txn requests for this epoch.
        int txn_id_offset = 0;
        while (!deconstructor_invoked_ &&
               GetTime() < epoch_start + epoch_duration_)
        {
            // Add next txn request to batch.
            if (batch.data_size() < MAX_LOCK_BATCH_SIZE)
            {
                TxnProtoExt *txn;
                string txn_string;
                client_->GetTxn(&txn,
                                batch_number * MAX_LOCK_BATCH_SIZE + txn_id_offset);

                if (txn->txn_id() == -1)
                {
                    delete txn;
                    continue;
                }

                // read-onlyトランザクションをDCCのバッチから分離
                if (txn->is_read_only())
                {
                    // read-onlyトランザクションは直接readerスレッドにプッシュ
                    read_only_queue_.Push(txn);
                }
                else
                {
                    // writeトランザクションはDCCバッチに含める
                    txn->SerializeToString(&txn_string);
                    batch.add_data(txn_string);
                    delete txn;
                }
                txn_id_offset++;
            }
        }

        std::string batch_string;
        batch.SerializeToString(&batch_string);

#ifdef PAXOS
        paxos.SubmitBatch(batch_string);
#else
        pthread_mutex_lock(&mutex_);
        batch_queue_.push(batch_string);
        pthread_mutex_unlock(&mutex_);
#endif
    }

    Spin(1);
}

void Sequencer::RunReader()
{
    PrintCpu("RunReader", 0);

    Spin(1);
#ifdef PAXOS
    Paxos paxos(ZOOKEEPER_CONF, true);
#endif

    map<int, MessageProto> batches;
    for (map<int, Node *>::iterator it = configuration_->all_nodes.begin();
         it != configuration_->all_nodes.end(); ++it)
    {
        batches[it->first].set_destination_channel("scheduler_");
        batches[it->first].set_destination_node(it->first);
        batches[it->first].set_type(MessageProto::TXN_BATCH);
    }

    // read-onlyトランザクションを送信するためのバッチメッセージ
    map<int, MessageProto> ro_batches;
    for (map<int, Node *>::iterator it = configuration_->all_nodes.begin();
         it != configuration_->all_nodes.end(); ++it)
    {
        ro_batches[it->first].set_destination_channel("scheduler_ro");
        ro_batches[it->first].set_destination_node(it->first);
        ro_batches[it->first].set_type(MessageProto::TXN_BATCH);
    }

    double time = GetTime();
    int txn_count = 0;
    int batch_count = 0;
    int batch_number = configuration_->this_node_id;

#ifdef LATENCY_TEST
    int watched_txn = -1;
#endif

    while (!deconstructor_invoked_)
    {
        // schedulerからのTXN_RETRYメッセージをチェック
        MessageProto retry_message;
        if (scheduler_connection_ && scheduler_connection_->GetMessage(&retry_message, ZMQ_NOBLOCK))
        {
            if (retry_message.type() == MessageProto::TXN_RETRY)
            {
                for (int i = 0; i < retry_message.data_size(); ++i)
                {
                    pthread_mutex_lock(&mutex_);
                    batch_queue_.push(retry_message.data(i));
                    pthread_mutex_unlock(&mutex_);
                }
            }
        }

        string batch_string;
        MessageProto batch_message;
#ifdef PAXOS
        paxos.GetNextBatchBlocking(&batch_string);
#else
        bool got_batch = false;
        do
        {
            pthread_mutex_lock(&mutex_);
            if (batch_queue_.size())
            {
                batch_string = batch_queue_.front();
                batch_queue_.pop();
                got_batch = true;
            }
            pthread_mutex_unlock(&mutex_);
            if (!got_batch)
                Spin(0.001);
        } while (!got_batch);
#endif
        batch_message.ParseFromString(batch_string);
        for (int i = 0; i < batch_message.data_size(); i++)
        {
            TxnProtoExt txn;
            txn.ParseFromString(batch_message.data(i));

#ifdef LATENCY_TEST
            if (txn.txn_id() % SAMPLE_RATE == 0)
                watched_txn = txn.txn_id();
#endif

            set<int> readers;
            set<int> writers;
            for (int j = 0; j < txn.read_set_size(); j++)
                readers.insert(configuration_->LookupPartition(txn.read_set(j)));
            for (int j = 0; j < txn.write_set_size(); j++)
                writers.insert(configuration_->LookupPartition(txn.write_set(j)));
            for (int j = 0; j < txn.read_write_set_size(); j++)
            {
                writers.insert(configuration_->LookupPartition(txn.read_write_set(j)));
                readers.insert(configuration_->LookupPartition(txn.read_write_set(j)));
            }

            for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
                txn.add_readers(*it);
            for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
                txn.add_writers(*it);

            bytes txn_data;
            txn.SerializeToString(&txn_data);

            for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
                readers.insert(*it);

            for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
                batches[*it].add_data(txn_data);

            txn_count++;
        }

        // read-onlyトランザクションを直接schedulerに送る
        TxnProtoExt *ro_txn;
        while (read_only_queue_.Pop(&ro_txn))
        {
            string ro_txn_string = ro_txn->SerializeAsString();

            set<int> ro_readers;
            if (ro_txn->read_set_size() > 0)
            {
                for (int j = 0; j < ro_txn->read_set_size(); j++)
                    ro_readers.insert(configuration_->LookupPartition(ro_txn->read_set(j)));
            }
            else
            {
                ro_readers.insert(configuration_->this_node_id);
            }

            for (set<int>::iterator it = ro_readers.begin(); it != ro_readers.end(); ++it)
                ro_batches[*it].add_data(ro_txn_string);

            txn_count++;
            delete ro_txn;
        }

        for (map<int, MessageProto>::iterator it = batches.begin();
             it != batches.end(); ++it)
        {
            it->second.set_batch_number(batch_number);
            connection_->Send(it->second);
            it->second.clear_data();
        }

        for (map<int, MessageProto>::iterator it = ro_batches.begin();
             it != ro_batches.end(); ++it)
        {
            it->second.set_batch_number(batch_number);
            connection_->Send(it->second);
            it->second.clear_data();
        }

        batch_number += configuration_->all_nodes.size();
        batch_count++;

#ifdef LATENCY_TEST
        if (watched_txn != -1)
        {
            sequencer_send[watched_txn] = GetTime();
            watched_txn = -1;
        }
#endif

        if (GetTime() > time + 1)
        {
#ifdef VERBOSE_SEQUENCER
            std::cout << "Submitted " << txn_count << " txns in " << batch_count
                      << " batches,\n"
                      << std::flush;
#endif
            time = GetTime();
            txn_count = 0;
            batch_count = 0;
        }
    }
    Spin(1);
}