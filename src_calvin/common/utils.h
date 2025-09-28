// Author: Alexander Thomson (thomson@cs.yale.edu)
// Author: Kun Ren (kun.ren@yale.edu)
//
// TODO(alex): UNIT TESTING!

#ifndef _DB_COMMON_UTILS_H_
#define _DB_COMMON_UTILS_H_

#include <assert.h>
#include <sys/time.h>
#include <unistd.h>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <cmath>
#include <vector>
#include <queue>
#include <tr1/unordered_map>

#include "common/types.h"

using std::string;
using std::vector;
using std::tr1::unordered_map;

#define ASSERTS_ON true

#define DCHECK(ARG) \
  do {              \
    if (ASSERTS_ON) \
      assert(ARG);  \
  } while (0)

// Status code for return values.
struct Status {
  // Represents overall status state.
  enum Code {
    ERROR = 0,
    OKAY = 1,
    DONE = 2,
  };
  Code code;

  // Optional explanation.
  string message;

  // Constructors.
  explicit Status(Code c) : code(c) {}
  Status(Code c, const string& s) : code(c), message(s) {}
  static Status Error() { return Status(ERROR); }
  static Status Error(const string& s) { return Status(ERROR, s); }
  static Status Okay() { return Status(OKAY); }
  static Status Done() { return Status(DONE); }

  // Pretty printing.
  string ToString() {
    string out;
    if (code == ERROR)
      out.append("Error");
    if (code == OKAY)
      out.append("Okay");
    if (code == DONE)
      out.append("Done");
    if (message.size()) {
      out.append(": ");
      out.append(message);
    }
    return out;
  }
};

// Returns the number of seconds since midnight according to local system time,
// to the nearest microsecond.
static inline double GetTime() {
  struct timeval tv;
  gettimeofday(&tv, NULL);
  return tv.tv_sec + tv.tv_usec / 1e6;
}

// Busy-wait for 'duration' seconds.
static inline void Spin(double duration) {
  usleep(1000000 * duration);
}

// Busy-wait until GetTime() >= time.
static inline void SpinUntil(double time) {
  while (GetTime() >= time) {
  }
}

// Produces a random alphabet string of the specified length
static inline string RandomString(int length) {
  string random_string;
  for (int i = 0; i < length; i++)
    random_string += rand() % 26 + 'A';

  return random_string;
}

// Returns a human-readable string representation of an int.
static inline string IntToString(int n) {
  char s[64];
  snprintf(s, sizeof(s), "%d", n);
  return string(s);
}

// Converts a human-readable numeric string to an int.
static inline int StringToInt(const string& s) {
  return atoi(s.c_str());
}

static inline string DoubleToString(double n) {
  char s[64];
  snprintf(s, sizeof(s), "%lf", n);
  return string(s);
}

static inline double StringToDouble(const string& s) {
  return atof(s.c_str());
}

static inline double RandomDoubleBetween(double fMin, double fMax) {
  double f = (double)rand() / RAND_MAX;
  return fMin + f * (fMax - fMin);
}

// Converts a human-readable numeric sub-string (starting at the 'n'th position
// of 's') to an int.
static inline int OffsetStringToInt(const string& s, int n) {
  return atoi(s.c_str() + n);
}

static inline void DeleteString(void* data, void* hint) {
  delete reinterpret_cast<string*>(hint);
}
static inline void Noop(void* data, void* hint) {}

////////////////////////////////
class Mutex {
 public:
  Mutex() { pthread_mutex_init(&mutex_, NULL); }
  ~Mutex() { pthread_mutex_destroy(&mutex_); }

 private:
  friend class Lock;
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  Mutex(const Mutex&);
  Mutex& operator=(const Mutex&);
};

class Lock {
 public:
  explicit Lock(Mutex* mutex) : mutex_(mutex) {
    pthread_mutex_lock(&mutex_->mutex_);
  }
  ~Lock() { pthread_mutex_unlock(&mutex_->mutex_); }

 private:
  Mutex* mutex_;
  Lock();
  Lock(const Lock&);
  Lock& operator=(const Lock&);
};

////////////////////////////////////////////////////////////////

template <typename T>
class AtomicQueue {
 public:
  AtomicQueue() { pthread_mutex_init(&mutex_, NULL); }
  ~AtomicQueue() { pthread_mutex_destroy(&mutex_); }

  inline void Push(const T& item) {
    pthread_mutex_lock(&mutex_);
    queue_.push(item);
    pthread_mutex_unlock(&mutex_);
  }

  inline bool Pop(T* result) {
    pthread_mutex_lock(&mutex_);
    if (queue_.empty()) {
      pthread_mutex_unlock(&mutex_);
      return false;
    }
    *result = queue_.front();
    queue_.pop();
    pthread_mutex_unlock(&mutex_);
    return true;
  }

  inline size_t Size() {
    pthread_mutex_lock(&mutex_);
    size_t size = queue_.size();
    pthread_mutex_unlock(&mutex_);
    return size;
  }

  inline bool Front(T* result) {
    pthread_mutex_lock(&mutex_);
    if (queue_.empty()) {
      pthread_mutex_unlock(&mutex_);
      return false;
    }
    *result = queue_.front();
    pthread_mutex_unlock(&mutex_);
    return true;
  }

 private:
  std::queue<T> queue_;
  pthread_mutex_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicQueue(const AtomicQueue<T>&);
  AtomicQueue& operator=(const AtomicQueue<T>&);
};

class MutexRW {
 public:
  MutexRW() { pthread_rwlock_init(&mutex_, NULL); }
  ~MutexRW() { pthread_rwlock_destroy(&mutex_); }

 private:
  friend class ReadLock;
  friend class WriteLock;
  pthread_rwlock_t mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  MutexRW(const MutexRW&);
  MutexRW& operator=(const MutexRW&);
};

class ReadLock {
 public:
  explicit ReadLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_rdlock(&mutex_->mutex_);
  }
  ~ReadLock() { pthread_rwlock_unlock(&mutex_->mutex_); }

 private:
  MutexRW* mutex_;
  ReadLock();
  ReadLock(const ReadLock&);
  ReadLock& operator=(const ReadLock&);
};

class WriteLock {
 public:
  explicit WriteLock(MutexRW* mutex) : mutex_(mutex) {
    pthread_rwlock_wrlock(&mutex_->mutex_);
  }
  ~WriteLock() { pthread_rwlock_unlock(&mutex_->mutex_); }

 private:
  MutexRW* mutex_;
  WriteLock();
  WriteLock(const WriteLock&);
  WriteLock& operator=(const WriteLock&);
};

template <typename K, typename V>
class AtomicMap {
 public:
  AtomicMap() {}
  ~AtomicMap() {}

  inline bool Lookup(const K& k, V* v) {
    ReadLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup == map_.end()) {
      return false;
    }
    *v = lookup->second;
    return true;
  }

  inline void Put(const K& k, const V& v) {
    WriteLock l(&mutex_);
    map_.insert(std::make_pair(k, v));
  }

  inline void Erase(const K& k) {
    WriteLock l(&mutex_);
    map_.erase(k);
  }

  inline V PutNoClobber(const K& k, const V& v) {
    WriteLock l(&mutex_);
    typename unordered_map<K, V>::const_iterator lookup = map_.find(k);
    if (lookup != map_.end()) {
      return lookup->second;
    }
    map_.insert(std::make_pair(k, v));
    return v;
  }

  inline uint32 Size() {
    ReadLock l(&mutex_);
    return map_.size();
  }

  inline void DeleteVAndClear() {
    WriteLock l(&mutex_);
    for (typename unordered_map<K, V>::iterator it = map_.begin();
         it != map_.end(); ++it) {
      delete it->second;
    }
    map_.clear();
  }

 private:
  unordered_map<K, V> map_;
  MutexRW mutex_;

  // DISALLOW_COPY_AND_ASSIGN
  AtomicMap(const AtomicMap<K, V>&);
  AtomicMap& operator=(const AtomicMap<K, V>&);
};

#endif  // _DB_COMMON_UTILS_H_