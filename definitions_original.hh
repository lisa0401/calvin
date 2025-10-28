#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== 元のCalvin用設定 (NUMA完全分離版) ==============
// ★ バックグラウンドをNode 1 (奇数)、ワーカーをNode 0 (偶数) に分離 ★
// =============================================================

// ============== server setting ==============
#define NUM_CORE 0 // スクリプトで上書き

// --- バックグラウンドスレッドの定義 (4つ) ---
#define NUM_BACKGROUND_THREADS 4
// バックグラウンドをNode 1 (奇数コア) にハードコードで割り当て
#define MULTIPLEXER_CORE       1
#define SEQUENCER_WRITER_CORE  3
#define SEQUENCER_READER_CORE  5
#define LOCK_MANAGER_CORE      7

// --- ワーカーの定義 ---
// ===============================================================
// Node0 = 偶数CPUを優先使用（Node1＝奇数CPUはBG専用）
// ただし、worker thread が 32 を超えた場合、
// 奇数CPU (Node1側) を 71 → 69 → 67 → ... の降順で順次割り当て。
// BGで予約済みの奇数コア（1,3,5,7）はスキップされます。
// ===============================================================

// Workerの総候補数（BGスレッドを除いた残り）
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)

// Node0 の論理CPU数（偶数CPUの数）
#define MAX_NODE0_LOGICAL   ((NUM_CORE) / 2)   // 例: 72なら36
// Node0 の物理コア数（物理:論理=1:2前提）
#define MAX_NODE0_PHYSICAL  ((NUM_CORE) / 4)   // 例: 72なら18

// ワーカー数の上限（Node0+Node1を含む安全上限）
#define MAX_SAFE_WORKERS   ((NUM_CORE) - (NUM_BACKGROUND_THREADS))
#define NUM_WORKERS        ((NUM_WORKERS_CORE) > (MAX_SAFE_WORKERS) ? (MAX_SAFE_WORKERS) : (NUM_WORKERS_CORE))

// ===============================================================
// BG専用コアをチェックする関数（このファイル内のBG定義のみを対象）
// ===============================================================
static inline bool IsReservedBgCore(int core) {
    if (core == MULTIPLEXER_CORE)      return true;
    if (core == SEQUENCER_WRITER_CORE) return true;
    if (core == SEQUENCER_READER_CORE) return true;
    if (core == LOCK_MANAGER_CORE)     return true;
    return false;
}

// ===============================================================
// 奇数CPUを高番から降順でスキャンして、BG専用コアを避けて割り当て
// ===============================================================
static inline int GetNthOddCoreDescSkippingBg(int n) {
    for (int c = NUM_CORE - 1; c >= 1; c -= 2) {
        if (IsReservedBgCore(c)) continue;
        if (n == 0) return c;
        --n;
    }
    return -1; // 枯渇時
}

// ===============================================================
// メイン割り当て関数：0〜31は偶数(Node0)、32以降は奇数(Node1降順)
// ===============================================================
static inline int GetWorkerCore(int thread_id) {
    const int phys = MAX_NODE0_PHYSICAL; // 例:18
    if (thread_id < 32) {
        // まずNode0(偶数コア)を使う
        if (thread_id < phys) {
            // 物理側
            return thread_id * 2;                          // 0,2,4,...,34
        } else {
            // SMT側
            return (thread_id - phys) * 2 + (NUM_CORE / 2); // 36,38,...,62
        }
    } else {
        // 32本を超えた分は奇数(Node1)降順で割り当て（BG除外）
        int idx = thread_id - 32;
        int odd = GetNthOddCoreDescSkippingBg(idx);
        if (odd >= 0) return odd;

        // 奇数コアを使い切った場合のフォールバック（重複回避のため一応ずらす）
        return ((thread_id - MAX_NODE0_PHYSICAL) * 2 + (NUM_CORE / 2)) % NUM_CORE;
    }
}

// ===============================================================
// 互換マクロ
// ===============================================================
#define GET_WORKER_CORE(thread_id) GetWorkerCore(thread_id)

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
