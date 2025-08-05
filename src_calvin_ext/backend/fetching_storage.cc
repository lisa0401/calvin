// Author: Philip Shao (philip.shao@yale.edu)
//
// An implementation of the storage interface taking into account
// main memory, disk, and swapping algorithms.

#include "backend/fetching_storage.h"
#include "common/definitions.hh" // Latch struct, State enum, PAGE_SIZE, STORAGE_PATH
#include <cstdio>
#include <cstdlib>
#include <string>
#include <iostream>
#include <memory>     // std::unique_ptr を使用するために追加
#include <functional> // std::hash のために追加
#include <pthread.h>  // pthread_mutex_t, pthread_cond_t, pthread_create, pthread_join
#include <unistd.h>   // usleep
#include <fcntl.h>    // open, O_RDONLY, O_NONBLOCK, O_CREAT, O_TRUNC, O_RDWR
#include <string.h>   // memset, strcpy, strrchr
#include <aio.h>      // aio_read, aio_write, aiocb, aio_error, sigval_t

// Latch struct and State enum should be defined in common/definitions.hh or fetching_storage.h
// For completeness, assuming they look like this:
/*
// In fetching_storage.h
class FetchingStorage : public Storage {
 public:
  enum State { UNINITIALIZED, IN_MEMORY, ON_DISK, FETCHING, RELEASING };
  class Latch {
   public:
    int active_requests;
    State state;
    pthread_mutex_t lock_;
    pthread_cond_t cond_; // This was added in fetching_storage.h
    Latch() {
      active_requests = 0;
      state = UNINITIALIZED;
      pthread_mutex_init(&lock_, NULL);
      pthread_cond_init(&cond_, NULL);
    }
    // Destructor for Latch to destroy mutex and cond (if not handled by FetchingStorage dtor)
    // ~Latch() {
    //   pthread_mutex_destroy(&lock_);
    //   pthread_cond_destroy(&cond_);
    // }
  };
  // ... rest of FetchingStorage class
};
*/

typedef FetchingStorage::Latch Latch;

////////////////// Constructors/Destructors  //////////////////////

// Singleton instance
FetchingStorage *FetchingStorage::self = NULL;

// Builds and returns the singleton instance of FetchingStorage.
FetchingStorage *FetchingStorage::BuildStorage()
{
    if (self == NULL)
    {
        self = new FetchingStorage();
    }
    return self;
}

// Private constructor: Initializes main memory storage, latches, and starts GC thread.
FetchingStorage::FetchingStorage()
{
    main_memory_ = new SimpleStorage();
    // Allocate 1 MILLION LATCHES!
    static const size_t LATCH_ARRAY_SIZE = 1000000;
    latches_ = new Latch[LATCH_ARRAY_SIZE];

    // Initialize mutexes and condition variables for all latches
    for (size_t i = 0; i < LATCH_ARRAY_SIZE; ++i)
    {
        // Latch constructor already calls pthread_mutex_init and pthread_cond_init
        // So, no need to call them again here unless Latch() is empty.
        // If Latch() is empty, uncomment the following two lines:
        // pthread_mutex_init(&latches_[i].lock_, NULL);
        // pthread_cond_init(&latches_[i].cond_, NULL);
        latches_[i].state = UNINITIALIZED; // Ensure state is initialized
        latches_[i].active_requests = 0;   // Ensure active requests is initialized
    }

    // Create and start the garbage collection thread
    pthread_create(&gc_thread_, NULL, RunGCThread, reinterpret_cast<void *>(this));
}

// Destructor: Cleans up allocated memory and joins the GC thread.
FetchingStorage::~FetchingStorage()
{
    // Signal GC thread to stop (assuming a mechanism for this, e.g., a flag)
    // For simplicity, we'll just detach/cancel for now, but a proper shutdown is needed.
    // pthread_cancel(gc_thread_); // Or use a shared flag and join
    // pthread_join(gc_thread_, NULL); // If using a flag to stop the thread

    delete main_memory_;

    // Destroy mutexes and condition variables
    static const size_t LATCH_ARRAY_SIZE = 1000000;
    for (size_t i = 0; i < LATCH_ARRAY_SIZE; ++i)
    {
        pthread_mutex_destroy(&latches_[i].lock_);
        pthread_cond_destroy(&latches_[i].cond_);
    }
    delete[] latches_;
}

////////////////// Private utility functions  //////////////////////

// LatchFor: Maps a key to a specific latch in the latches_ array using hashing.
// This is crucial for correct concurrency control.
Latch *FetchingStorage::LatchFor(const Key &key)
{
    static const size_t LATCH_ARRAY_SIZE = 1000000;
    // Use std::hash to get a more robust hash for string keys,
    // then take modulo to fit within the array size.
    // This replaces the problematic atoi(key.c_str())
    size_t index = std::hash<std::string>{}(key) % LATCH_ARRAY_SIZE;
    return latches_ + index;
}

// GetKey: Extracts the key (filename) from a file descriptor's path.
void FetchingStorage::GetKey(int fd, Key *key)
{
    char path[255];
    snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
    char key_c_str[255];
    memset(&key_c_str, 0, 255);
    // readlink reads the target of the symlink (e.g., /path/to/STORAGE_PATH/key_name)
    ssize_t bytes_read = readlink(path, key_c_str, sizeof(key_c_str) - 1); // Ensure null termination
    if (bytes_read != -1)
    {
        key_c_str[bytes_read] = '\0'; // Explicitly null-terminate
        const char *last_slash = strrchr(key_c_str, '/');
        if (last_slash)
        {
            *key = string(last_slash + 1);
        }
        else
        {
            *key = string(key_c_str); // No slash, treat whole path as key
        }
    }
    else
    {
        *key = ""; // Error reading symlink, return empty key
        std::cerr << "Error reading symlink for fd " << fd << std::endl;
    }
}

// RunGCThread: The main loop for the garbage collection thread.
// It periodically unfetches cold data.
void *FetchingStorage::RunGCThread(void *arg)
{
    FetchingStorage *storage = reinterpret_cast<FetchingStorage *>(arg);
    while (true)
    {
        double start_time = GetTime();
        // Iterate through a range of keys to potentially unfetch them.
        // COLD_CUTOFF is assumed to be defined in common/definitions.hh
        for (int i = COLD_CUTOFF; i < 1000000; i++)
        {
            storage->HardUnfetch(IntToString(i));
        }
        // Sleep for the remaining time to maintain a consistent GC cycle.
        usleep(static_cast<int>(1000000 * (GetTime() - start_time)));
    }
    return NULL;
}

///////////// The meat and potato public interface methods.  ///////////

// ReadObject: Reads an object from storage. Blocks if the object is being fetched.
Value *FetchingStorage::ReadObject(const Key &key, int64 txn_id)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    // Block thread until pre-fetch on this key is done.
    // Use a condition variable to avoid busy-waiting.
    while (latch->state == FETCHING)
    {
        pthread_cond_wait(&latch->cond_, &latch->lock_);
    }

    // Assertions to ensure correct state after prefetch is complete.
    // These asserts should be true if Prefetch and callbacks work correctly.
    assert(latch->state != ON_DISK && "ReadObject called on ON_DISK object without prefetch");
    assert(latch->state != RELEASING && "ReadObject called on RELEASING object");
    assert(latch->state == IN_MEMORY && "ReadObject called on non-IN_MEMORY object after waiting");
    assert(latch->active_requests > 0 && "ReadObject called without active request");

    pthread_mutex_unlock(&latch->lock_);

    // Read the object from main memory.
    // main_memory_->ReadObject is assumed to return a new Value* (copy)
    // that the caller is responsible for deleting (or managing with unique_ptr).
    return main_memory_->ReadObject(key);
}

// PutObject: Writes data to main memory.
// It assumes ownership of the 'value' pointer passed to it.
// backend/fetching_storage.cc の PutObject 関数を以下に置き換えてください

bool FetchingStorage::PutObject(const Key &key, Value *value, int64_t txn_id)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    // 修正: ガベージコレクタとの競合を避けるため、ラッチの状態がRELEASINGでも
    // 書き込みを許可するようにアサーションを修正します。
    assert(latch->active_requests > 0 || latch->state == UNINITIALIZED ||
           latch->state == IN_MEMORY || latch->state == RELEASING);

    latch->state = IN_MEMORY; // 状態をIN_MEMORYに設定
    pthread_mutex_unlock(&latch->lock_);

    main_memory_->PutObject(key, value);
    return true;
}

// DeleteObject: Deletes an object from main memory and updates latch state.
// This is the corrected version that avoids NULL pointer dereference.
bool FetchingStorage::DeleteObject(const Key &key, int64 txn_id)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    latch->active_requests--;            // Decrement active requests for this key
    assert(latch->active_requests >= 0); // Ensure it doesn't go negative

    // If no active requests, mark for release or on disk.
    if (latch->active_requests == 0)
    {
        // If it was in memory, it's now effectively uninitialized in main memory.
        if (latch->state == IN_MEMORY || latch->state == RELEASING)
        {
            latch->state = UNINITIALIZED; // Or ON_DISK if it was persisted
        }
    }
    pthread_mutex_unlock(&latch->lock_);

    // Delete the object from main memory.
    // main_memory_->DeleteObject is assumed to handle its own memory management.
    return main_memory_->DeleteObject(key);
}

// Prefetch: Initiates fetching of an object into main memory if it's not already there.
// Returns false if the file does not exist on disk.
bool FetchingStorage::Prefetch(const Key &key, double *wait_time)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    int active_requests = latch->active_requests;
    latch->active_requests++; // Increment active requests for this prefetch

    State previous_state = latch->state;
    State current_state = previous_state; // Initialize current_state

    if (previous_state == ON_DISK || previous_state == UNINITIALIZED)
    {
        // If on disk or uninitialized, we need to fetch.
        current_state = FETCHING;
        latch->state = FETCHING;
    }
    else if (previous_state == RELEASING)
    {
        // If it was releasing, bring it back to IN_MEMORY.
        current_state = IN_MEMORY;
        latch->state = IN_MEMORY;
    }
    // If already IN_MEMORY or FETCHING, state remains the same.

    pthread_mutex_unlock(&latch->lock_);

    // Handle different states outside the lock.
    if (current_state == IN_MEMORY)
    {
        *wait_time = 0; // Already in memory, no wait.
        return true;
    }
    else if (current_state == FETCHING && previous_state == FETCHING)
    {
        // Another prefetching attempt is already in progress.
        *wait_time = 0.100; // Arbitrary non-zero wait time.
        return true;
    }
    else if (current_state == FETCHING && (previous_state == ON_DISK || previous_state == UNINITIALIZED))
    {
        // Cold call to prefetch: initiate file read.
        assert(active_requests == 0);    // Should be the first active request for this fetch
        *wait_time = 0.100;              // Somewhat larger arbitrary non-zero wait time.
        char *buf = new char[PAGE_SIZE]; // Buffer for AIO read
        if (!FileRead(key, buf, PAGE_SIZE))
        {
            delete[] buf; // Clean up buffer if FileRead fails
            // If FileRead fails (e.g., file not found), update latch state.
            pthread_mutex_lock(&latch->lock_);
            latch->state = UNINITIALIZED;          // Or ON_DISK if it was known to be there
            latch->active_requests--;              // Decrement as this request failed
            pthread_cond_broadcast(&latch->cond_); // Notify waiting threads that fetch failed
            pthread_mutex_unlock(&latch->lock_);
            return false;
        }
        return true;
    }
    return false; // Should not reach here in normal flow
}

// HardUnfetch: Forces an object out of main memory to disk.
// Handles memory cleanup for the Value object.
bool FetchingStorage::HardUnfetch(const Key &key)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    State previous_state = latch->state;
    int active_requests = latch->active_requests;

    // Only transition to RELEASING or ON_DISK if no active requests.
    if (active_requests == 0)
    {
        if (latch->state == IN_MEMORY)
        { // Only release if it's currently in memory
            latch->state = RELEASING;
        }
        else if (latch->state == FETCHING)
        { // If fetching but no active requests, means fetch was cancelled/failed
            latch->state = ON_DISK;
        }
    }
    pthread_mutex_unlock(&latch->lock_);

    if (active_requests == 0 && previous_state == IN_MEMORY)
    {
        // Read the object from main memory before writing to disk.
        // Use unique_ptr to ensure proper deletion of the returned Value*.
        std::unique_ptr<Value> result_ptr(main_memory_->ReadObject(key));
        if (result_ptr)
        { // Check if ReadObject returned a valid pointer
            int len = result_ptr->data.length();
            // Allocate buffer for C-style string, including null terminator.
            char *c_result = new char[len + 1];
            strcpy(c_result, result_ptr->data.c_str());

            bool success = FilePut(key, c_result, len); // Write to file
            delete[] c_result;                          // Free the buffer after use.
            return success;
        }
        return false; // Could not read object from main memory
    }
    else
    {
        return true; // No action needed or already not in memory
    }
}

// Unfetch: Decrements active requests for a key. If requests drop to zero,
// it might trigger a HardUnfetch.
bool FetchingStorage::Unfetch(const Key &key)
{
    Latch *latch = LatchFor(key);
    pthread_mutex_lock(&latch->lock_);

    // Decrement active requests.
    latch->active_requests--;
    assert(latch->active_requests >= 0); // Ensure active_requests doesn't go negative

    // Assertions for expected states during unfetch.
    assert(latch->state == FETCHING || latch->state == RELEASING ||
           latch->state == IN_MEMORY || latch->state == UNINITIALIZED); // UNINITIALIZED added

    // If active requests drop to zero, and it's in memory, consider unfetching.
    if (latch->active_requests == 0 && latch->state == IN_MEMORY)
    {
        latch->state = RELEASING; // Mark for potential release
    }
    else if (latch->active_requests == 0 && latch->state == FETCHING)
    {
        latch->state = ON_DISK; // If fetching and no requests, assume fetch won't complete.
    }

    pthread_mutex_unlock(&latch->lock_);

    // HardUnfetch might be called by the GC thread, or explicitly here
    // if state becomes UNINITIALIZED or RELEASING and active_requests is 0.
    // The original code called HardUnfetch if state was UNINITIALIZED, which is odd.
    // The GC thread is responsible for actual HardUnfetch based on COLD_CUTOFF.
    // This function primarily manages active_requests and state transitions.
    return true;
}

///////////////// Asynchronous Callbacks ////////////////////////

// PrefetchCompletionHandler: Callback for asynchronous read (prefetch) completion.
void FetchingStorage::PrefetchCompletionHandler(sigval_t sigval)
{
    struct aiocb *req;
    req = (struct aiocb *)sigval.sival_ptr;

    // Check if the AIO request completed successfully.
    if (aio_error(req) == 0)
    {
        // Get the key associated with this request.
        string key;
        char *buf = const_cast<char *>(reinterpret_cast<volatile char *>(req->aio_buf));
        GetKey(req->aio_fildes, &key);

        FetchingStorage *store = FetchingStorage::BuildStorage();
        Latch *latch = store->LatchFor(key);

        pthread_mutex_lock(&latch->lock_);
        State prev_state = latch->state;
        latch->state = IN_MEMORY;              // Mark the object as now in memory.
        pthread_cond_broadcast(&latch->cond_); // Notify any waiting threads in ReadObject.
        pthread_mutex_unlock(&latch->lock_);

        // If nothing interfered with our fetch (i.e., it was still FETCHING),
        // put the fetched data into main memory.
        if (prev_state == FETCHING)
        {
            // Create a new Value object from the fetched buffer and pass ownership.
            // Assuming Value constructor can take char* and size, or std::string.
            store->main_memory_->PutObject(key, new Value(std::string(buf, PAGE_SIZE)));
        }
        delete[] buf;           // Free the buffer allocated for AIO.
        close(req->aio_fildes); // Close the file descriptor.
        delete req;             // Free the aiocb structure.
    }
    else
    {
        // Handle AIO error (e.g., file not found, I/O error).
        // Need to update latch state to reflect fetch failure.
        struct aiocb *error_req = (struct aiocb *)sigval.sival_ptr;
        string key;
        char *buf = const_cast<char *>(reinterpret_cast<volatile char *>(error_req->aio_buf));
        GetKey(error_req->aio_fildes, &key);

        FetchingStorage *store = FetchingStorage::BuildStorage();
        Latch *latch = store->LatchFor(key);

        pthread_mutex_lock(&latch->lock_);
        // Mark as ON_DISK or UNINITIALIZED if fetch failed.
        latch->state = ON_DISK;                // Or UNINITIALIZED if it was a new object
        latch->active_requests--;              // Decrement as this request failed to complete successfully
        pthread_cond_broadcast(&latch->cond_); // Notify waiting threads
        pthread_mutex_unlock(&latch->lock_);

        delete[] buf;
        close(error_req->aio_fildes);
        delete error_req;
        std::cerr << "Prefetch AIO error for key: " << key << std::endl;
    }
}

// UnfetchCompletionHandler: Callback for asynchronous write (unfetch) completion.
void FetchingStorage::UnfetchCompletionHandler(sigval_t sigval)
{
    struct aiocb *req;
    req = (struct aiocb *)sigval.sival_ptr;

    // Check if the AIO request completed successfully.
    if (aio_error(req) == 0)
    {
        // Get the key.
        string key;
        GetKey(req->aio_fildes, &key);

        FetchingStorage *store = FetchingStorage::BuildStorage();
        Latch *latch = store->LatchFor(key);

        pthread_mutex_lock(&latch->lock_);
        // Ensure active_requests is not negative.
        assert(latch->active_requests >= 0);
        int active_requests = latch->active_requests;
        State state = latch->state;
        pthread_mutex_unlock(&latch->lock_);

        // If it was releasing and no active requests, it's now fully on disk.
        if (state == RELEASING && active_requests <= 0)
        {
            store->main_memory_->DeleteObject(key); // Delete from main memory.
            latch->state = ON_DISK;                 // Mark as on disk.
        }
        close(req->aio_fildes);                                   // Close file descriptor.
        delete[] reinterpret_cast<volatile char *>(req->aio_buf); // Free buffer.
        delete req;                                               // Free aiocb structure.
    }
    else
    {
        // Handle AIO error for unfetch.
        struct aiocb *error_req = (struct aiocb *)sigval.sival_ptr;
        string key;
        char *buf = const_cast<char *>(reinterpret_cast<volatile char *>(error_req->aio_buf));
        GetKey(error_req->aio_fildes, &key);

        // Consider what state to transition to if unfetch fails.
        // Maybe stay IN_MEMORY or log a critical error.
        std::cerr << "Unfetch AIO error for key: " << key << std::endl;

        delete[] buf;
        close(error_req->aio_fildes);
        delete error_req;
    }
    return;
}

/*
 * Here live the bogus hacks. (File I/O wrappers for AIO)
 */

// FileRead: Initiates an asynchronous read operation.
bool FetchingStorage::FileRead(const Key &key, char *result, int size)
{
    string fileName(STORAGE_PATH);
    fileName.append(key);
    int fd = open(fileName.c_str(), O_RDONLY | O_NONBLOCK);
    if (fd == -1)
    {
        perror(("open for read failed for key: " + key).c_str());
        return false;
    }
    return aio_read(generateControlBlock(fd, result, size, FETCH)) >= 0;
}

// FilePut: Initiates an asynchronous write operation.
bool FetchingStorage::FilePut(const Key &key, char *value, int size)
{
    string fileName(STORAGE_PATH);
    fileName.append(key);
    mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
    int fd = open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_NONBLOCK, mode);
    if (fd == -1)
    {
        perror(("open for write failed for key: " + key).c_str());
        return false;
    }
    return aio_write(generateControlBlock(fd, value, size, RELEASE)) >= 0;
}

// generateControlBlock: Helper to create and configure an aiocb structure.
aiocb *FetchingStorage::generateControlBlock(int fd,
                                             char *buf,
                                             const int size,
                                             Operation op)
{
    aiocb *aiocbp = new aiocb();
    aiocbp->aio_fildes = fd;
    aiocbp->aio_offset = 0;
    aiocbp->aio_buf = buf;
    aiocbp->aio_nbytes = size;
    aiocbp->aio_reqprio = 0;
    /* Link the AIO request with a thread callback */
    aiocbp->aio_sigevent.sigev_notify = SIGEV_THREAD;
    if (op == FETCH)
        aiocbp->aio_sigevent.sigev_notify_function = &PrefetchCompletionHandler;
    else
        aiocbp->aio_sigevent.sigev_notify_function = &UnfetchCompletionHandler;
    aiocbp->aio_sigevent.sigev_notify_attributes = NULL;
    aiocbp->aio_sigevent.sigev_value.sival_ptr = aiocbp;
    return aiocbp;
}

// // Author: Philip Shao (philip.shao@yale.edu)
// //
// // An implementation of the storage interface taking into account
// // main memory, disk, and swapping algorithms.

// #include "backend/fetching_storage.h"
// #include "common/definitions.hh"

// typedef FetchingStorage::Latch Latch;

// ////////////////// Constructors/Destructors  //////////////////////

// // Singleton constructor

// FetchingStorage* FetchingStorage::self = NULL;

// FetchingStorage* FetchingStorage::BuildStorage() {
//   if (self == NULL)
//     self = new FetchingStorage();
//   return self;
// }

// // Private constructor

// FetchingStorage::FetchingStorage() {
//   main_memory_ = new SimpleStorage();
//   // 1 MILLION LATCHES!
//   latches_ = new Latch[1000000];

//   pthread_create(&gc_thread_, NULL, RunGCThread, reinterpret_cast<void*>(this));
// }

// FetchingStorage::~FetchingStorage() {
//   delete main_memory_;
//   delete[] latches_;
// }

// ////////////////// Private utility functions  //////////////////////

// Latch* FetchingStorage::LatchFor(const Key& key) {
//   // Just fail miserably if we are passed a non-int.
//   // assert(atoi(key.c_str()) != 0);
//   // An array is a nice threadsafe hashtable.
//   return latches_ + atoi(key.c_str());
// }

// void FetchingStorage::GetKey(int fd, Key* key) {
//   char path[255];
//   snprintf(path, sizeof(path), "/proc/self/fd/%d", fd);
//   char key_c_str[255];
//   memset(&key_c_str, 0, 255);
//   readlink(path, key_c_str, 255);
//   *key = string((strrchr(key_c_str, '/') + 1));
// }

// void* FetchingStorage::RunGCThread(void* arg) {
//   FetchingStorage* storage = reinterpret_cast<FetchingStorage*>(arg);
//   while (true) {
//     double start_time = GetTime();
//     for (int i = COLD_CUTOFF; i < 1000000; i++) {
//       storage->HardUnfetch(IntToString(i));
//     }
//     usleep(static_cast<int>(1000000 * (GetTime() - start_time)));
//   }
//   return NULL;
// }

// ///////////// The meat and potato public interface methods.  ///////////

// Value* FetchingStorage::ReadObject(const Key& key, int64 txn_id) {
//   Latch* latch = LatchFor(key);
//   // Must call a Prefetch before transaction.
//   pthread_mutex_lock(&latch->lock_);
//   assert(latch->state != ON_DISK);
//   assert(latch->state != RELEASING);
//   assert(latch->state == FETCHING || latch->state == IN_MEMORY);
//   assert(latch->active_requests > 0);
//   pthread_mutex_unlock(&latch->lock_);
//   // Block thread until pre-fetch on this key is done.
//   while (latch->state == FETCHING) {
//   }
//   return main_memory_->ReadObject(key);
// }

// // Write data to memory.
// bool FetchingStorage::PutObject(const Key& key, Value* value, int64 txn_id) {
//   Latch* latch = LatchFor(key);
//   pthread_mutex_lock(&latch->lock_);
//   // Must call a prefetch before transaction
//   assert(latch->active_requests > 0);
//   latch->state = IN_MEMORY;
//   pthread_mutex_unlock(&latch->lock_);
//   main_memory_->PutObject(key, new Value(*value));
//   return true;
// }

// // Put null, change state to uninitialized.
// bool FetchingStorage::DeleteObject(const Key& key, int64 txn_id) {
//   return PutObject(key, NULL, txn_id);
// }

// // Return false if file does not exist.
// bool FetchingStorage::Prefetch(const Key& key, double* wait_time) {
//   Latch* latch = LatchFor(key);

//   pthread_mutex_lock(&latch->lock_);

//   int active_requests = latch->active_requests;
//   latch->active_requests++;

//   State previous_state = latch->state;
//   if (previous_state == ON_DISK)
//     latch->state = FETCHING;
//   if (previous_state == UNINITIALIZED) {
//     main_memory_->PutObject(key, new Value());
//     latch->state = IN_MEMORY;
//   }
//   if (previous_state == RELEASING)
//     latch->state = IN_MEMORY;
//   State current_state = latch->state;

//   pthread_mutex_unlock(&latch->lock_);

//   // Pre-fetch and in memory pre-states are no-ops.
//   if (current_state == IN_MEMORY) {
//     *wait_time = 0;  // You're good to go.
//     return true;
//   } else if (previous_state == FETCHING) {
//     // We already have another prefetching attempt.
//     *wait_time = 0.100;  // arbitrary nonzero result.
//     return true;
//   } else {
//     // Not in memory: cold call to prefetch.
//     assert(active_requests == 0);
//     *wait_time = 0.100;  // somewhat larger arbitrary nonzero result.
//     char* buf = new char[PAGE_SIZE];
//     return FileRead(key, buf, PAGE_SIZE);
//   }
// }

// bool FetchingStorage::HardUnfetch(const Key& key) {
//   // Since we have a write lock, we know there are no concurrent
//   // reads to this key, so we can freely read as well.
//   Latch* latch = LatchFor(key);
//   pthread_mutex_lock(&latch->lock_);

//   State previous_state = latch->state;
//   int active_requests = latch->active_requests;

//   // Only one of the following two conditions can be true.
//   if (active_requests == 0)
//     latch->state = RELEASING;
//   if (active_requests == 0 && latch->state == FETCHING)
//     latch->state = ON_DISK;

//   pthread_mutex_unlock(&latch->lock_);

//   if (active_requests == 0 && previous_state == IN_MEMORY) {
//     Value* result = main_memory_->ReadObject(key);
//     int len = result->data.length();
//     char* c_result = new char[len + 1];
//     strcpy(c_result, result->data.c_str());
//     return FilePut(key, c_result, len);
//   } else {
//     return true;
//   }
// }

// bool FetchingStorage::Unfetch(const Key& key) {
//   Latch* latch = LatchFor(key);
//   pthread_mutex_lock(&latch->lock_);
//   State state = latch->state;
//   latch->active_requests--;
//   assert(latch->active_requests >= 0);
//   assert(latch->state == FETCHING || latch->state == RELEASING ||
//          latch->state == IN_MEMORY);
//   pthread_mutex_unlock(&latch->lock_);
//   if (state == UNINITIALIZED)
//     HardUnfetch(key);
//   return true;
// }

// ///////////////// Asynchronous Callbacks ////////////////////////

// void FetchingStorage::PrefetchCompletionHandler(sigval_t sigval) {
//   struct aiocb* req;
//   req = (struct aiocb*)sigval.sival_ptr;
//   /* Did the request complete? */
//   if (aio_error(req) == 0) {
//     /* Request completed successfully, get the return status */
//     string key;
//     char* buf =
//         const_cast<char*>(reinterpret_cast<volatile char*>(req->aio_buf));
//     GetKey(req->aio_fildes, &key);
//     FetchingStorage* store = FetchingStorage::BuildStorage();
//     Latch* latch = store->LatchFor(key);
//     pthread_mutex_lock(&latch->lock_);
//     State prev_state = latch->state;
//     latch->state = IN_MEMORY;
//     pthread_mutex_unlock(&latch->lock_);
//     /* Nothing interfered with our fetch */
//     if (prev_state == FETCHING) {
//       string* value = new string(buf, PAGE_SIZE);
//       store->main_memory_->PutObject(key, new Value(*value));
//     }
//     delete[] buf;
//     close(req->aio_fildes);
//     delete req;
//   }
// }

// void FetchingStorage::UnfetchCompletionHandler(sigval_t sigval) {
//   struct aiocb* req;
//   req = (struct aiocb*)sigval.sival_ptr;
//   /* Did the request complete? */
//   if (aio_error(req) == 0) {
//     /* Request completed successfully, get the return status */
//     string key;
//     GetKey(req->aio_fildes, &key);
//     FetchingStorage* store = FetchingStorage::BuildStorage();
//     Latch* latch = store->LatchFor(key);
//     pthread_mutex_lock(&latch->lock_);
//     // Hasn't been fetched.
//     assert(latch->active_requests >= 0);
//     int active_requests = latch->active_requests;
//     State state = latch->state;
//     pthread_mutex_unlock(&latch->lock_);
//     if (state == RELEASING && active_requests <= 0) {
//       store->main_memory_->DeleteObject(key);
//       latch->state = ON_DISK;
//     }
//     close(req->aio_fildes);
//     delete[] reinterpret_cast<volatile char*>(req->aio_buf);
//     delete req;
//   }
//   return;
// }

// /*
//  * Here live the bogus hacks.
//  */

// bool FetchingStorage::FileRead(const Key& key, char* result, int size) {
//   string fileName(STORAGE_PATH);
//   fileName.append(key);
//   int fd = open(fileName.c_str(), O_RDONLY | O_NONBLOCK);
//   if (fd == -1)
//     return false;
//   return aio_read(generateControlBlock(fd, result, size, FETCH)) >= 0;
// }

// bool FetchingStorage::FilePut(const Key& key, char* value, int size) {
//   string fileName(STORAGE_PATH);
//   fileName.append(key);
//   mode_t mode = S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH;
//   int fd =
//       open(fileName.c_str(), O_RDWR | O_CREAT | O_TRUNC | O_NONBLOCK, mode);
//   if (fd == -1)
//     return false;
//   return aio_write(generateControlBlock(fd, value, size, RELEASE)) >= 0;
// }

// aiocb* FetchingStorage::generateControlBlock(int fd,
//                                              char* buf,
//                                              const int size,
//                                              Operation op) {
//   aiocb* aiocbp = new aiocb();
//   aiocbp->aio_fildes = fd;
//   aiocbp->aio_offset = 0;
//   aiocbp->aio_buf = buf;
//   aiocbp->aio_nbytes = size;
//   aiocbp->aio_reqprio = 0;
//   /* Link the AIO request with a thread callback */
//   aiocbp->aio_sigevent.sigev_notify = SIGEV_THREAD;
//   if (op == FETCH)
//     aiocbp->aio_sigevent.sigev_notify_function = &PrefetchCompletionHandler;
//   else
//     aiocbp->aio_sigevent.sigev_notify_function = &UnfetchCompletionHandler;
//   aiocbp->aio_sigevent.sigev_notify_attributes = NULL;
//   aiocbp->aio_sigevent.sigev_value.sival_ptr = aiocbp;
//   return aiocbp;
// }
