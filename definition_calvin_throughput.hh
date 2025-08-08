#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== ★★★ 修正箇所 ★★★ ==============
// CPUコアの割り当て戦略を、よりスケーラブルなものに変更

// ============== server setting ==============
// NUM_COREはビルドスクリプトによって上書きされるが、コンパイル時のためにデフォルト値を設定
#define NUM_CORE 8

// バックグラウンド処理用のコア数。RO Dispatcherが追加されたため5に更新
#define NUM_BACKGROUND_THREADS 5

// バックグラウンドスレッドを、番号の大きいコアに固定で割り当てる
#define MAIN_PROCESS_CORE (NUM_CORE - 1)
#define MULTIPLEXER_CORE (NUM_CORE - 2)
#define SEQUENCER_WRITER_CORE (NUM_CORE - 3)
#define SEQUENCER_READER_CORE (NUM_CORE - 4)
#define LOCK_MANAGER_CORE (NUM_CORE - 5)
// #define RO_DISPATCHER_CORE (NUM_CORE - 6) // ROディスパッチャも同様に割り当て

// ワーカー用のコア数とスレッド数を定義
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)
#define NUM_WORKERS (NUM_WORKERS_CORE)

// ワーカーを、番号の小さいコアから順番に詰めて割り当てる
// これにより、コア数の上限まで正しくスケールする
// clang-format off
#define GET_WORKER_CORE(thread_id) (thread_id)
// clang-format on
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
