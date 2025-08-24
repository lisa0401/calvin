// sequencer/sequencer.h

#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include <pthread.h> // pthread関連の型のために追加
#include <queue>
#include <set>
#include <string>
#include <vector>

#include "common/definitions.hh"
#include "proto/message.pb.h"

using std::queue;
using std::set;
using std::string;
using std::vector;

class Configuration;
class Connection;
class Storage;
class TxnProto;

class Client
{
public:
    virtual ~Client() {}
    virtual void GetTxn(TxnProto **txn, int txn_id) = 0;
};

class Sequencer
{
public:
    Sequencer(Configuration *conf, Connection *rw_connection,
              vector<Connection *> *ro_connections, Client *client,
              Storage *storage);

    ~Sequencer();

private:
    // --- スレッド実行関数 ---
    void RunWriter();
    void RunReader();
    void RunGenerator();

    // --- スレッドのエントリーポイント (static) ---
    static void *RunSequencerWriter(void *arg);
    static void *RunSequencerReader(void *arg);
    static void *RunSequencerGenerator(void *arg);

    void FindParticipatingNodes(const TxnProto &txn, set<int> *nodes);

    // --- メンバー変数 ---
    double epoch_duration_;
    Configuration *configuration_;
    Connection *rw_connection_;
    vector<Connection *> *ro_connections_;
    Client *client_;
    Storage *storage_;
    bool deconstructor_invoked_;

    // --- スレッドハンドル ---
    pthread_t writer_thread_;
    pthread_t reader_thread_;
    pthread_t generator_thread_;

    // --- 内部キュー (Writer -> Reader) ---
    queue<MessageProto *> batch_queue_;
    pthread_mutex_t mutex_; // batch_queue_用 mutex

    // --- 内部キュー (Generator -> Writer) ---
    queue<TxnProto *> txn_queue_;
    pthread_mutex_t txn_queue_mutex_; // txn_queue_用 mutex

    // --- ▼ 修正: コンディション変数を追加 ▼ ---
    pthread_cond_t queue_not_full_cond_;  // キューが満杯でないことを通知
    pthread_cond_t queue_not_empty_cond_; // キューが空でないことを通知
    // --- ▲ 修正 ▲ ---
};

#endif // _DB_SEQUENCER_SEQUENCER_H_