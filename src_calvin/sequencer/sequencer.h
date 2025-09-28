// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.

#ifndef _DB_SEQUENCER_SEQUENCER_H_
#define _DB_SEQUENCER_SEQUENCER_H_

#include <set>
#include <string>
#include <queue>

#include "common/definitions.hh"

#define SAMPLES 100000
#define SAMPLE_RATE 999

using std::queue;
using std::set;
using std::string;

class Configuration;
class Connection;
class Storage;
class TxnProto;
class Client;
class Scheduler; // Schedulerクラスの前方宣言

class Client {
 public:
  virtual ~Client() {}
  virtual void GetTxn(TxnProto** txn, int txn_id) = 0;
};

class Sequencer {
 public:
  Sequencer(Configuration* conf,
            Connection* connection,
            Client* client,
            Storage* storage,
            Scheduler* scheduler);
  ~Sequencer();

 private:
  // Sequencer's main loops.
  void Run();
  void GeneratorLoop();

  // Functions to start the main loops, called in new pthreads.
  static void* RunSequencer(void* arg);
  static void* RunGenerator(void* arg);

  void FindParticipatingNodes(const TxnProto& txn, set<int>* nodes);

  double epoch_duration_;
  Configuration* configuration_;
  Connection* connection_;
  Client* client_;
  Storage* storage_;
  Scheduler* scheduler_;

  pthread_t sequencer_thread_;
  pthread_t generator_thread_;

  bool deconstructor_invoked_;
  
  queue<TxnProto*> txn_queue_;
  pthread_mutex_t txn_queue_mutex_;
  pthread_cond_t txn_queue_cond_;
};
#endif  // _DB_SEQUENCER_SEQUENCER_H_