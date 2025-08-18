// sequencer/sequencer.h (修正後)

#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include <set>
#include <string>
#include <queue>
#include <vector> // ★ vectorを追加
#include "proto/message.pb.h"
#include "common/definitions.hh"

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
    // ★ コンストラクタ: Connection* を vector<Connection*>* に変更
    Sequencer(Configuration *conf,
              Connection *rw_connection,
              vector<Connection *> *ro_connections,
              Client *client,
              Storage *storage);

    ~Sequencer();

private:
    void RunWriter();
    void RunReader();
    static void *RunSequencerWriter(void *arg);
    static void *RunSequencerReader(void *arg);
    void FindParticipatingNodes(const TxnProto &txn, set<int> *nodes);

    double epoch_duration_;
    Configuration *configuration_;

    // ★ メンバー変数: Connection* を vector<Connection*>* に変更
    Connection *rw_connection_;
    vector<Connection *> *ro_connections_; // 複数のRO接続を保持

    Client *client_;
    Storage *storage_;
    pthread_t writer_thread_;
    pthread_t reader_thread_;
    bool deconstructor_invoked_;
    queue<MessageProto *> batch_queue_;
    pthread_mutex_t mutex_;
};
#endif // _DB_SEQUENCER_SEQUENCER_H_