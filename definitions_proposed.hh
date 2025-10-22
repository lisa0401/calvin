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
#define LOCK_MANAGER_CORE 5
#define SEQUENCER_GENERATOR_CORE 7
#define SNAPSHOT_THREAD_CORE 11
#define GET_RO_DISPATCHER_CORE(i) (13 + 2 * (i))


// --- ワーカーの定義 ---
// Node0 = 偶数CPUのみを使用（奇数CPUは常にBG専用）
// 物理優先: まず Node0 の「物理側」18本 → その後に SMT 側18本
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)

// Node0 の論理CPU数（= 全体の半分 = 偶数CPUの数）
#define MAX_NODE0_LOGICAL   ( (NUM_CORE) / 2 )   // 72なら36
// Node0 の物理コア数（= 論理/2）
#define MAX_NODE0_PHYSICAL  ( (NUM_CORE) / 4 )   // 72なら18

// ワーカー数は Node0 論理数を上限にクランプ（奇数=Node1は使わない）
#define NUM_WORKERS ( (NUM_WORKERS_CORE) > (MAX_NODE0_LOGICAL) ? (MAX_NODE0_LOGICAL) : (NUM_WORKERS_CORE) )

// 割り当て順：
//   0..(MAX_NODE0_PHYSICAL-1)   → 物理側  偶数: 0,2,4,..., (2*18-2=34)
//   MAX_NODE0_PHYSICAL..(NUM_WORKERS-1) → SMT側 偶数: 36,38,40,...,70
#define GET_WORKER_CORE(thread_id) \
    ( ((thread_id) < (MAX_NODE0_PHYSICAL)) \
        ? ((thread_id) * 2) \
        : (((thread_id) - (MAX_NODE0_PHYSICAL)) * 2 + (NUM_CORE)/2) )



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