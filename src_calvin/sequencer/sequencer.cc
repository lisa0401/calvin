// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// The sequencer component of the system is responsible for choosing a global
// serial order of transactions to which execution must maintain equivalence.

#include "sequencer/sequencer.h"
#include "scheduler/deterministic_scheduler.h"

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

using std::map;
using std::multimap;
using std::queue;
using std::set;


void* Sequencer::RunSequencer(void* arg) {
  reinterpret_cast<Sequencer*>(arg)->Run();
  return NULL;
}

void* Sequencer::RunGenerator(void* arg) {
  reinterpret_cast<Sequencer*>(arg)->GeneratorLoop();
  return NULL;
}

Sequencer::Sequencer(Configuration* conf,
                     Connection* connection,
                     Client* client,
                     Storage* storage,
                     Scheduler* scheduler)
    : epoch_duration_(EPOCH_DURATION),
      configuration_(conf),
      connection_(connection),
      client_(client),
      storage_(storage),
      scheduler_(scheduler),
      deconstructor_invoked_(false) {
  pthread_mutex_init(&txn_queue_mutex_, NULL);
  pthread_cond_init(&txn_queue_cond_, NULL);

  cpu_set_t cpuset;

  // Generatorスレッドを起動
  pthread_attr_t attr_generator;
  pthread_attr_init(&attr_generator);
  CPU_ZERO(&cpuset);
  CPU_SET(SEQUENCER_GENERATOR_CORE, &cpuset);
  pthread_attr_setaffinity_np(&attr_generator, sizeof(cpu_set_t), &cpuset);
  pthread_create(&generator_thread_, &attr_generator, RunGenerator, reinterpret_cast<void*>(this));

  // Sequencerスレッドを起動
  pthread_attr_t attr_sequencer;
  pthread_attr_init(&attr_sequencer);
  CPU_ZERO(&cpuset);
  CPU_SET(SEQUENCER_CORE, &cpuset);
  pthread_attr_setaffinity_np(&attr_sequencer, sizeof(cpu_set_t), &cpuset);
  pthread_create(&sequencer_thread_, &attr_sequencer, RunSequencer, reinterpret_cast<void*>(this));
}

Sequencer::~Sequencer() {
  deconstructor_invoked_ = true;
  
  pthread_mutex_lock(&txn_queue_mutex_);
  pthread_cond_broadcast(&txn_queue_cond_);
  pthread_mutex_unlock(&txn_queue_mutex_);

  pthread_join(generator_thread_, NULL);
  pthread_join(sequencer_thread_, NULL);
  
  pthread_mutex_lock(&txn_queue_mutex_);
  while (!txn_queue_.empty()) {
    delete txn_queue_.front();
    txn_queue_.pop();
  }
  pthread_mutex_unlock(&txn_queue_mutex_);
  
  pthread_mutex_destroy(&txn_queue_mutex_);
  pthread_cond_destroy(&txn_queue_cond_);
}

void Sequencer::FindParticipatingNodes(const TxnProto& txn, set<int>* nodes) {
  nodes->clear();
  for (int i = 0; i < txn.read_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_set(i)));
  for (int i = 0; i < txn.write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.write_set(i)));
  for (int i = 0; i < txn.read_write_set_size(); i++)
    nodes->insert(configuration_->LookupPartition(txn.read_write_set(i)));
}

void Sequencer::GeneratorLoop() {
  uint64 txn_id_counter = 0;
  while (!deconstructor_invoked_) {
    TxnProto* txn;
    client_->GetTxn(&txn, txn_id_counter++);

    if (txn->txn_id() == -1) {
      delete txn;
      continue;
    }

    pthread_mutex_lock(&txn_queue_mutex_);
    txn_queue_.push(txn);
    pthread_cond_signal(&txn_queue_cond_);
    pthread_mutex_unlock(&txn_queue_mutex_);
  }
}

void Sequencer::Run() {
  PrintCpu("RunSequencer", 0);
  
  std::cout << "Starting sequencer.\n" << std::flush;

  while (!deconstructor_invoked_) {
    pthread_mutex_lock(&txn_queue_mutex_);
    
    while (txn_queue_.empty() && !deconstructor_invoked_) {
        pthread_cond_wait(&txn_queue_cond_, &txn_queue_mutex_);
    }

    if (deconstructor_invoked_) {
        pthread_mutex_unlock(&txn_queue_mutex_);
        break;
    }

    TxnProto* txn = txn_queue_.front();
    txn_queue_.pop();
    pthread_mutex_unlock(&txn_queue_mutex_);

    set<int> readers;
    set<int> writers;
    // ★★★ ここから下の 'txn.' をすべて 'txn->' に修正 ★★★
    for (int j = 0; j < txn->read_set_size(); j++)
      readers.insert(configuration_->LookupPartition(txn->read_set(j)));
    for (int j = 0; j < txn->write_set_size(); j++)
      writers.insert(configuration_->LookupPartition(txn->write_set(j)));
    for (int j = 0; j < txn->read_write_set_size(); j++) {
      int partition = configuration_->LookupPartition(txn->read_write_set(j));
      writers.insert(partition);
      readers.insert(partition);
    }
    for (set<int>::iterator it = readers.begin(); it != readers.end(); ++it)
      txn->add_readers(*it);
    for (set<int>::iterator it = writers.begin(); it != writers.end(); ++it)
      txn->add_writers(*it);
    
    scheduler_->PostTxn(txn);
  }
}

