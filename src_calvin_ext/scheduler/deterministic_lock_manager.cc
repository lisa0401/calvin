// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun@cs.yale.edu)
//
// Lock manager implementing deterministic two-phase locking as described in
// 'The Case for Determinism in Database Systems'.

#include "scheduler/deterministic_lock_manager.h"

#include <vector>

#include "proto/txn.pb.h"
#include "backend/txn_proto_ext.h"

using std::vector;

DeterministicLockManager::DeterministicLockManager(deque<TxnProtoExt *> *ready_txns,
                                                   Configuration *config)
    : configuration_(config), ready_txns_(ready_txns)
{
    for (int i = 0; i < LOCK_TABLE_SIZE; i++)
        lock_table_[i] = new deque<KeysList>();
}

int DeterministicLockManager::Lock(TxnProtoExt *txn)
{
    int not_acquired = 0;

    for (int i = 0; i < txn->read_write_set_size(); i++)
    {
        if (IsLocal(txn->read_write_set(i)))
        {
            deque<KeysList> *key_requests = lock_table_[Hash(txn->read_write_set(i))];

            deque<KeysList>::iterator it;
            for (it = key_requests->begin();
                 it != key_requests->end() && it->key != txn->read_write_set(i);
                 ++it)
            {
            }
            deque<LockRequest> *requests;
            if (it == key_requests->end())
            {
                requests = new deque<LockRequest>();
                key_requests->push_back(KeysList(txn->read_write_set(i), requests));
            }
            else
            {
                requests = it->locksrequest;
            }

            if (requests->empty() || txn != requests->back().txn)
            {
                requests->push_back(LockRequest(WRITE, txn));
                if (requests->size() > 1)
                    not_acquired++;
            }
        }
    }

    // ✅ 必ず Lock 関数の末尾に含めること
    if (not_acquired > 0)
        txn_waits_[txn] = not_acquired;
    else
        ready_txns_->push_back(txn);
    return not_acquired;
}

void DeterministicLockManager::Release(TxnProtoExt *txn)
{
    // Currently commented out because nothing in any write set can conflict
    // in TPCC or Microbenchmark.
    //  for (int i = 0; i < txn->write_set_size(); i++)
    //    if (IsLocal(txn->write_set(i)))
    //      Release(txn->write_set(i), txn);
    for (int i = 0; i < txn->read_write_set_size(); i++)
        if (IsLocal(txn->read_write_set(i)))
            Release(txn->read_write_set(i), txn);
}

void DeterministicLockManager::Release(const Key &key, TxnProtoExt *txn)
{
    // Avoid repeatedly looking up key in the unordered_map.
    deque<KeysList> *key_requests = lock_table_[Hash(key)];

    deque<KeysList>::iterator it1;
    for (it1 = key_requests->begin();
         it1 != key_requests->end() && it1->key != key; ++it1)
    {
    }
    deque<LockRequest> *requests = it1->locksrequest;

    // Seek to the target request. Note whether any write lock requests precede
    // the target.
    bool write_requests_precede_target = false;
    deque<LockRequest>::iterator it;
    for (it = requests->begin(); it != requests->end() && it->txn != txn; ++it)
    {
        if (it->mode == WRITE)
            write_requests_precede_target = true;
    }

    // If we found the request, erase it. No need to do anything otherwise.
    if (it != requests->end())
    {
        // Save an iterator pointing to the target to call erase on after handling
        // lock inheritence, since erase(...) trashes all iterators.
        deque<LockRequest>::iterator target = it;

        // If there are more requests following the target request, one or more
        // may need to be granted as a result of the target's release.
        ++it;
        if (it != requests->end())
        {
            vector<TxnProtoExt *> new_owners;
            // Grant subsequent request(s) if:
            //  (a) The canceled request held a write lock.
            //  (b) The canceled request held a read lock ALONE.
            //  (c) The canceled request was a write request preceded only by read
            //      requests and followed by one or more read requests.
            if (target == requests->begin() &&
                (target->mode == WRITE ||
                 (target->mode == READ && it->mode == WRITE)))
            { // (a) or (b)
                // If a write lock request follows, grant it.
                if (it->mode == WRITE)
                    new_owners.push_back(it->txn);
                // If a sequence of read lock requests follows, grant all of them.
                for (; it != requests->end() && it->mode == READ; ++it)
                    new_owners.push_back(it->txn);
            }
            else if (!write_requests_precede_target && target->mode == WRITE &&
                     it->mode == READ)
            { // (c)
                // If a sequence of read lock requests follows, grant all of them.
                for (; it != requests->end() && it->mode == READ; ++it)
                    new_owners.push_back(it->txn);
            }

            // Handle txns with newly granted requests that may now be ready to run.
            for (uint64 j = 0; j < new_owners.size(); j++)
            {
                txn_waits_[new_owners[j]]--;
                if (txn_waits_[new_owners[j]] == 0)
                {
                    // The txn that just acquired the released lock is no longer waiting
                    // on any lock requests.
                    ready_txns_->push_back(new_owners[j]);
                    txn_waits_.erase(new_owners[j]);
                }
            }
        }

        // Now it is safe to actually erase the target request.
        requests->erase(target);
        if (requests->size() == 0)
        {
            delete requests;
            key_requests->erase(it1);
        }
    }
}
