// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.
//
// TODO(scw): replace iostream with cstdio
#define VERBOSE_SEQUENCER
#include "sequencer/sequencer.h"

#include <iostream>
#include <map>
#include <queue>
#include <set>
#include <utility>

#include "backend/storage.h"
#include "common/configuration.h"
#include "common/connection.h"
#include "common/utils.h"
#include "common/debug.hh"
#include "proto/message.pb.h"
#include "proto/txn.pb.h"
#ifdef PAXOS
#include "paxos/paxos.h"
#endif

using std::map;
using std::multimap;
using std::queue;
using std::set;

#ifdef LATENCY_TEST
double sequencer_recv[SAMPLES];
double sequencer_send[SAMPLES];
double prefetch_cold[SAMPLES];
double scheduler_lock[SAMPLES];
double worker_begin[SAMPLES];
double worker_end[SAMPLES];
double scheduler_unlock[SAMPLES];
#endif
// In sequencer.cc

// 新しいスレッドのエントリーポイント
void *Sequencer::RunSequencerGenerator(void *arg)
{
    reinterpret_cast<Sequencer *>(arg)->RunGenerator();
    return NULL;
}

Sequencer::Sequencer(Configuration *conf,
                     Connection *rw_connection,
                     vector<Connection *> *ro_connections,
                     Client *client,
                     Storage *storage)
    : epoch_duration_(EPOCH_DURATION),
      configuration_(conf),
      rw_connection_(rw_connection),
      ro_connections_(ro_connections),
      client_(client),
      storage_(storage),
      deconstructor_invoked_(false)
{
    // 新しいミューテックスと条件変数を初期化
    pthread_mutex_init(&txn_queue_mutex_, NULL);
    pthread_cond_init(&queue_not_full_cond_, NULL);
    pthread_cond_init(&queue_not_empty_cond_, NULL);

    cpu_set_t cpuset;

    // Writerスレッドの生成
    pthread_attr_t attr_writer;
    pthread_attr_init(&attr_writer);
    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_WRITER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
    pthread_create(&writer_thread_, &attr_writer, RunSequencerWriterReader, reinterpret_cast<void *>(this));

    // Generatorスレッドの生成
    pthread_attr_t attr_generator;
    pthread_attr_init(&attr_generator);
    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_GENERATOR_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_generator, sizeof(cpu_set_t), &cpuset);
    pthread_create(&generator_thread_, &attr_generator, RunSequencerGenerator, reinterpret_cast<void *>(this));
}
Sequencer::~Sequencer()
{
    // 1. 全てのスレッドに終了を通知する
    deconstructor_invoked_ = true;

    // 2. cond_waitで待機(スリープ)中のスレッドを全て起こす
    pthread_cond_broadcast(&queue_not_full_cond_);
    pthread_cond_broadcast(&queue_not_empty_cond_);

    // 3. 全てのスレッドが終了するのを待つ
    pthread_join(generator_thread_, NULL);
    pthread_join(writer_thread_, NULL);

    // 4. キューに残ったトランザクションを解放し、メモリリークを防ぐ
    pthread_mutex_lock(&txn_queue_mutex_);
    while (!txn_queue_.empty())
    {
        delete txn_queue_.front();
        txn_queue_.pop();
    }
    pthread_mutex_unlock(&txn_queue_mutex_);

    // 5. 使用したミューテックスとコンディション変数を全て破棄する
    pthread_mutex_destroy(&txn_queue_mutex_);
    pthread_cond_destroy(&queue_not_full_cond_);
    pthread_cond_destroy(&queue_not_empty_cond_);
}

void Sequencer::FindParticipatingNodes(const TxnProto &txn, set<int> *nodes)
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
double PrefetchAll(Storage *storage, TxnProto *txn)
{
    double max_wait_time = 0;
    double wait_time = 0;
    for (int i = 0; i < txn->read_set_size(); i++)
    {
        storage->Prefetch(txn->read_set(i), &wait_time);
        max_wait_time = MAX(max_wait_time, wait_time);
    }
    for (int i = 0; i < txn->read_write_set_size(); i++)
    {
        storage->Prefetch(txn->read_write_set(i), &wait_time);
        max_wait_time = MAX(max_wait_time, wait_time);
    }
    for (int i = 0; i < txn->write_set_size(); i++)
    {
        storage->Prefetch(txn->write_set(i), &wait_time);
        max_wait_time = MAX(max_wait_time, wait_time);
    }
#ifdef LATENCY_TEST
    if (txn->txn_id() % SAMPLE_RATE == 0)
        prefetch_cold[txn->txn_id() / SAMPLE_RATE] = max_wait_time;
#endif
    return max_wait_time;
}
#endif

// トランザクションを事前に生成しておくキューの上限サイズ
#define MAX_TXN_QUEUE_SIZE 50000

void Sequencer::RunGenerator()
{
    PrintCpu("RunGenerator", 0);
    uint64_t txn_id_counter = 0;

    while (!deconstructor_invoked_)
    {
        pthread_mutex_lock(&txn_queue_mutex_);
        // キューが一杯なら、空きが出るまで待機
        while (txn_queue_.size() >= MAX_TXN_QUEUE_SIZE && !deconstructor_invoked_)
        {
            pthread_cond_wait(&queue_not_full_cond_, &txn_queue_mutex_);
        }
        pthread_mutex_unlock(&txn_queue_mutex_);

        if (deconstructor_invoked_) break;

        TxnProto *txn = nullptr;
        client_->GetTxn(&txn, txn_id_counter++);

        if (txn != nullptr)
        {
            pthread_mutex_lock(&txn_queue_mutex_);
            txn_queue_.push(txn);
            // キューに新しい要素が入ったことをWriterに通知
            pthread_cond_signal(&queue_not_empty_cond_);
            pthread_mutex_unlock(&txn_queue_mutex_);
        }
    }
}

// 新エントリポイント
void* Sequencer::RunSequencerWriterReader(void* arg) {
    reinterpret_cast<Sequencer*>(arg)->RunWriterReader();
    return NULL;
}

// Writer + Reader を統合
void Sequencer::RunWriterReader() {
    PrintCpu("RunWriterReader", 0);

    // ノード別のRWバッチを常設（送信毎にdata_ptrのみクリア）
    std::map<int, MessageProto> rw_batches;
    for (auto it = configuration_->all_nodes.begin();
         it != configuration_->all_nodes.end(); ++it) {
        rw_batches[it->first].set_destination_channel("scheduler_");
        rw_batches[it->first].set_destination_node(it->first);
        rw_batches[it->first].set_type(MessageProto::TXN_BATCH);
    }

    uint64_t next_dispatcher = 0;
    int batch_number = configuration_->this_node_id;

#ifdef VERBOSE_SEQUENCER
    double stat_time = GetTime();
    int stat_txn_count = 0, stat_batch_count = 0;
    double total_ro_latency = 0.0;
    int ro_txn_count = 0;
#endif

    while (!deconstructor_invoked_) {
        const double epoch_start = GetTime();

        // ROバッチ（このepoch内でまとめて送る）
        MessageProto ro_batch_message;
        ro_batch_message.set_type(MessageProto::TXN_BATCH);
        std::string ro_channel_name = "ro_scheduler_" + IntToString(next_dispatcher);
        ro_batch_message.set_destination_channel(ro_channel_name);
        std::set<int> ro_dest_nodes; // ROを配る先のノード集合

        int batched = 0;

        // ====== Writer（txn_queue_から取り出し/上限・時間まで詰める）======
        while (!deconstructor_invoked_ && GetTime() < epoch_start + epoch_duration_) {
            if (batched >= MAX_LOCK_BATCH_SIZE) break;

            TxnProto* txn = nullptr;

            // Generator との1対1のProducer-Consumer
            pthread_mutex_lock(&txn_queue_mutex_);
            while (txn_queue_.empty() && !deconstructor_invoked_) {
                pthread_cond_wait(&queue_not_empty_cond_, &txn_queue_mutex_);
            }
            if (!txn_queue_.empty()) {
                txn = txn_queue_.front();
                txn_queue_.pop();
                pthread_cond_signal(&queue_not_full_cond_);
            }
            pthread_mutex_unlock(&txn_queue_mutex_);

            if (!txn || deconstructor_invoked_) break;
            if (txn->txn_id() == -1) { delete txn; continue; }

            txn->set_sequencer_start_time(GetTime());
            txn->set_read_only(txn->write_set_size() == 0 && txn->read_write_set_size() == 0);

            // ====== Readerの役割（その場でRO/RWへ振り分け）======
            std::set<int> readers, writers;
            for (int j = 0; j < txn->read_set_size(); j++)
                readers.insert(configuration_->LookupPartition(txn->read_set(j)));
            for (int j = 0; j < txn->write_set_size(); j++)
                writers.insert(configuration_->LookupPartition(txn->write_set(j)));
            for (int j = 0; j < txn->read_write_set_size(); j++) {
                int p = configuration_->LookupPartition(txn->read_write_set(j));
                readers.insert(p);
                writers.insert(p);
            }
            for (int p : readers) txn->add_readers(p);
            for (int p : writers) txn->add_writers(p);

            if (txn->read_only()) {
#ifdef VERBOSE_SEQUENCER
                if (txn->has_sequencer_start_time()) {
                    total_ro_latency += (GetTime() - txn->sequencer_start_time());
                    ro_txn_count++;
                }
#endif
                ro_batch_message.add_data_ptr(
                    static_cast<uint64_t>(reinterpret_cast<uintptr_t>(txn)));
                for (int p : readers) ro_dest_nodes.insert(p);
            } else {
                const uint64_t txn_ptr =
                    static_cast<uint64_t>(reinterpret_cast<uintptr_t>(txn));
                std::set<int> participants = readers;
                participants.insert(writers.begin(), writers.end());
                for (int p : participants) {
                    rw_batches[p].add_data_ptr(txn_ptr);
                }
                // 注意：txn の所有権は以後スケジューラ側へ（ここで delete しない）
            }

            batched++;
#ifdef VERBOSE_SEQUENCER
            stat_txn_count++;
#endif
        }

        // ====== 送信：RO → 複数ノードへ複製配信 ======
        if (ro_batch_message.data_ptr_size() > 0) {
            for (int node : ro_dest_nodes) {
                ro_batch_message.set_destination_node(node);
                (*ro_connections_)[next_dispatcher]->Send(ro_batch_message);
            }
            next_dispatcher = (next_dispatcher + 1) % ro_connections_->size();
        }

        // ====== 送信：RW → 各ノードの scheduler_ へ ======
        for (auto &kv : rw_batches) {
            if (kv.second.data_ptr_size() > 0) {
                kv.second.set_batch_number(batch_number);
                rw_connection_->Send(kv.second);
                kv.second.clear_data_ptr(); // メッセージ本体は再利用
            }
        }

        // DCCのためのグローバル順序（従来通り）
        batch_number += configuration_->all_nodes.size();

#ifdef VERBOSE_SEQUENCER
        stat_batch_count++;
        if (GetTime() > stat_time + 1) {
            std::cout << "Submitted " << stat_txn_count
                      << " txns in " << stat_batch_count << " batches.\n";
            if (ro_txn_count > 0) {
                double avg = total_ro_latency / ro_txn_count;
                std::cout << "Average RO Txn Sequencer's latency for this epoch: "
                          << avg << " seconds.\n";
            }
            stat_time = GetTime();
            stat_txn_count = 0;
            stat_batch_count = 0;
            total_ro_latency = 0.0;
            ro_txn_count = 0;
        }
#endif
    }
}
