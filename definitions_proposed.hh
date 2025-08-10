#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== 提案手法Calvin用設定 (NUMA完全分離版) ==============
// ★ バックグラウンドをNode 1 (奇数)、ワーカーをNode 0 (偶数) に分離 ★
// =================================================================

// ============== server setting ==============
#define NUM_CORE 0 // スクリプトで上書き

// --- バックグラウンドスレッドの定義 ---
#define NUM_RO_DISPATCHERS 1 // スクリプトで上書きされる
#define OTHER_BACKGROUND_THREADS 4
#define NUM_BACKGROUND_THREADS (OTHER_BACKGROUND_THREADS + NUM_RO_DISPATCHERS)

// バックグラウンドスレッドをNode 1 (奇数コア) にハードコードで割り当て
#define MULTIPLEXER_CORE 1
#define SEQUENCER_WRITER_CORE 3
#define SEQUENCER_READER_CORE 5
#define LOCK_MANAGER_CORE 7
#define GET_RO_DISPATCHER_CORE(i) (9 + (i) * 2) // 続番の奇数コアを割り当て

// --- ワーカーの定義 ---
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)
#define NUM_WORKERS (NUM_WORKERS_CORE)
// ワーカーをNode 0 (偶数コア) に割り当て
#define GET_WORKER_CORE(thread_id) ((thread_id) * 2)
// ==============================================

// ============== database setting ==============
#define DB_SIZE 1000000
#define LOCK_TABLE_SIZE 1000000
// ==============================================

// ============== workload setting ==============
#define RW_SET_SIZE 100
#define SKEW 0.99
#define UNIFORM_KEY_SELECTION_RATIO 0
#define HOT 10
// ==============================================

// ============== used for only calvin ==============
#define MAX_ACTIVE_TXNS 2000
#define LOCK_BATCH_SIZE 100
// ==============================================