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
    // 既存のミューテックスを初期化
    pthread_mutex_init(&mutex_, NULL);
    // 新しいミューテックスを初期化
    pthread_mutex_init(&txn_queue_mutex_, NULL);

    cpu_set_t cpuset;

    // Writerスレッドの生成
    pthread_attr_t attr_writer;
    pthread_attr_init(&attr_writer);
    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_WRITER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_writer, sizeof(cpu_set_t), &cpuset);
    pthread_create(&writer_thread_, &attr_writer, RunSequencerWriter, reinterpret_cast<void *>(this));

    // Readerスレッドの生成
    pthread_attr_t attr_reader;
    pthread_attr_init(&attr_reader);
    CPU_ZERO(&cpuset);
    CPU_SET(SEQUENCER_READER_CORE, &cpuset);
    pthread_attr_setaffinity_np(&attr_reader, sizeof(cpu_set_t), &cpuset);
    pthread_create(&reader_thread_, &attr_reader, RunSequencerReader, reinterpret_cast<void *>(this));

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
    //    これをしないと、スレッドが起きずにjoinで永久に待ち続ける(デッドロック)可能性がある
    pthread_cond_broadcast(&queue_not_full_cond_);
    pthread_cond_broadcast(&queue_not_empty_cond_);

    // 3. 全てのスレッドが終了するのを待つ
    pthread_join(generator_thread_, NULL);
    pthread_join(writer_thread_, NULL);
    pthread_join(reader_thread_, NULL);

    // 4. キューに残ったトランザクションを解放し、メモリリークを防ぐ
    pthread_mutex_lock(&txn_queue_mutex_);
    while (!txn_queue_.empty())
    {
        delete txn_queue_.front();
        txn_queue_.pop();
    }
    pthread_mutex_unlock(&txn_queue_mutex_);

    // 5. 使用したミューテックスとコンディション変数を全て破棄する
    pthread_mutex_destroy(&mutex_);
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

// In sequencer.cc

// トランザクションを事前に生成しておくキューの上限サイズ
#define MAX_TXN_QUEUE_SIZE 50000

void Sequencer::RunGenerator()
{
    PrintCpu("RunGenerator", 0);

    while (!deconstructor_invoked_)
    {
        // キューが一杯なら少し待つ
        pthread_mutex_lock(&txn_queue_mutex_);
        bool is_full = txn_queue_.size() >= MAX_TXN_QUEUE_SIZE;
        pthread_mutex_unlock(&txn_queue_mutex_);

        if (is_full)
        {
            Spin(0.001); // 1ms待機
            continue;
        }

        // トランザクションを生成
        // GetTxnの第二引数は、もはやバッチ番号と無関係なので0としておくか、
        // clientの実装に合わせて調整する
        TxnProto *txn = nullptr;
        client_->GetTxn(&txn, 0);

        if (txn != nullptr)
        {
            // 生成したトランザクションをキューに追加
            pthread_mutex_lock(&txn_queue_mutex_);
            txn_queue_.push(txn);
            pthread_mutex_unlock(&txn_queue_mutex_);
        }
    }
}

void Sequencer::RunWriter()
{
    PrintCpu("RunWriter", 0);

    // ... (Paxosや同期のロジックは変更なし) ...

    // === エポックループ ===
    for (int batch_number = configuration_->this_node_id; !deconstructor_invoked_;
         batch_number += configuration_->all_nodes.size())
    {
        const double epoch_start = GetTime();

        MessageProto *batch = new MessageProto();
        batch->set_type(MessageProto::TXN_BATCH);
        batch->set_destination_channel("sequencer");
        batch->set_destination_node(-1);
        batch->set_batch_number(batch_number);

        while (!deconstructor_invoked_ && GetTime() < epoch_start + epoch_duration_)
        {
            if (batch->data_ptr_size() >= MAX_LOCK_BATCH_SIZE)
                break;

            // --- ▼ 変更 ▼ ---
            // client_->GetTxn の代わりにキューから取得
            TxnProto *txn = nullptr;

            pthread_mutex_lock(&txn_queue_mutex_);
            if (!txn_queue_.empty())
            {
                txn = txn_queue_.front();
                txn_queue_.pop();
            }
            pthread_mutex_unlock(&txn_queue_mutex_);
            // --- ▲ 変更 ▲ ---

            // ロードジェネレータが「空」を返す場合、またはキューが空だった場合のスキップ
            if (txn == nullptr || txn->txn_id() == -1)
            {
                if (txn)
                    delete txn;
                // キューが空だった場合は少し待機してリトライ
                if (txn == nullptr)
                    Spin(0.0001);
                continue;
            }

            // Sequencer 側で最小限の属性設定
            txn->set_sequencer_start_time(GetTime());
            txn->set_read_only(txn->write_set_size() == 0 && txn->read_write_set_size() == 0);

            // シリアライズせず、生ポインタを data_ptr に積む
            batch->add_data_ptr(static_cast<uint64_t>(reinterpret_cast<uintptr_t>(txn)));
        }

        // 内部キューへ投入（RunReader 側で取り出し/分配）
        pthread_mutex_lock(&mutex_);
        batch_queue_.push(batch);
        pthread_mutex_unlock(&mutex_);
    }
}

void Sequencer::RunReader()
{
    PrintCpu("RunReader", 0);

    map<int, MessageProto> rw_batches;
    for (auto it = configuration_->all_nodes.begin(); it != configuration_->all_nodes.end(); ++it)
    {
        rw_batches[it->first].set_destination_channel("scheduler_");
        rw_batches[it->first].set_destination_node(it->first);
        rw_batches[it->first].set_type(MessageProto::TXN_BATCH);
    }

    uint64_t next_dispatcher = 0;
    double time = GetTime();
    int txn_count = 0, batch_count = 0;
    int batch_number = configuration_->this_node_id;
    double total_ro_latency = 0.0;
    int ro_txn_count = 0;

    while (!deconstructor_invoked_)
    {
        MessageProto *batch_message = nullptr;

        // ★ 文字列でなく MessageProto* を取り出す
        bool got_batch = false;
        do
        {
            pthread_mutex_lock(&mutex_);
            if (!batch_queue_.empty())
            {
                batch_message = batch_queue_.front();
                batch_queue_.pop();
                got_batch = true;
            }
            pthread_mutex_unlock(&mutex_);
            if (!got_batch)
                Spin(0.001);
        } while (!got_batch);

        // ★ RO バッチは data_ptr で組み立てる
        MessageProto ro_batch_message;
        ro_batch_message.set_type(MessageProto::TXN_BATCH);
        string ro_channel_name = "ro_scheduler_" + IntToString(next_dispatcher);
        ro_batch_message.set_destination_channel(ro_channel_name);
        set<int> ro_dest_nodes;

        // ★ 各 TxnProto* を取り出し
        for (int i = 0; i < batch_message->data_ptr_size(); i++)
        {
            auto raw = batch_message->data_ptr(i);
            TxnProto *txn = reinterpret_cast<TxnProto *>(static_cast<uintptr_t>(raw));

            set<int> readers, writers;
            for (int j = 0; j < txn->read_set_size(); j++)
                readers.insert(configuration_->LookupPartition(txn->read_set(j)));
            for (int j = 0; j < txn->write_set_size(); j++)
                writers.insert(configuration_->LookupPartition(txn->write_set(j)));
            for (int j = 0; j < txn->read_write_set_size(); j++)
            {
                auto p = configuration_->LookupPartition(txn->read_write_set(j));
                writers.insert(p);
                readers.insert(p);
            }

            for (int p : readers)
                txn->add_readers(p);
            for (int p : writers)
                txn->add_writers(p);

            if (txn->read_only())
            {
                if (txn->has_sequencer_start_time())
                {
                    total_ro_latency += (GetTime() - txn->sequencer_start_time());
                    ro_txn_count++;
                }
                // ★ pointer をそのまま中継（RO専用、同一プロセス前提）
                ro_batch_message.add_data_ptr(static_cast<uint64_t>(reinterpret_cast<uintptr_t>(txn)));
                for (int p : readers)
                    ro_dest_nodes.insert(p);
            }
            else
            {
                // ★ RW はネットワーク/Connection 経路なのでシリアライズに戻す
                bytes txn_data;
                txn->SerializeToString(&txn_data);
                for (int p : writers)
                    readers.insert(p); // 既存ロジック
                for (int p : readers)
                    rw_batches[p].add_data(txn_data);

                // ★ RW はここで責務終了なので delete（RO は中継先で delete）
                delete txn;
            }

            txn_count++;
        }

        // ★ RO の送信（同一プロセス内 Connection 前提。RO Dispatcher は data_ptr を読む実装に）
        if (ro_batch_message.data_ptr_size() > 0)
        {
            for (int node : ro_dest_nodes)
            {
                ro_batch_message.set_destination_node(node);
                (*ro_connections_)[next_dispatcher]->Send(ro_batch_message);
            }
            next_dispatcher = (next_dispatcher + 1) % ro_connections_->size();
        }

        // ★ RW の送信（従来通り bytes）
        for (auto it = rw_batches.begin(); it != rw_batches.end(); ++it)
        {
            if (it->second.data_size() > 0)
            {
                it->second.set_batch_number(batch_number);
                rw_connection_->Send(it->second);
                it->second.clear_data();
            }
        }
        batch_number += configuration_->all_nodes.size();
        batch_count++;

        // ★ 使用済みバッチメッセージを解放（中の Txn は RO のみ残っているが、これは下流で解放）
        delete batch_message;

        if (GetTime() > time + 1)
        {
#ifdef VERBOSE_SEQUENCER
            std::cout << "Submitted " << txn_count << " txns in " << batch_count << " batches.\n";
            if (ro_txn_count > 0)
            {
                double avg = total_ro_latency / ro_txn_count;
                std::cout << "Average RO Txn Sequencer's latency for this epoch: " << avg << " seconds.\n";
            }
#endif
            time = GetTime();
            txn_count = 0;
            batch_count = 0;
            total_ro_latency = 0.0;
            ro_txn_count = 0;
        }
    }
}
