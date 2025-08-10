#pragma once

// ============== please don't change ==============
#define COLD_CUTOFF 1000000
#define MAX_LOCK_BATCH_SIZE 2000
#define EPOCH_DURATION 0.01 // 0.01 is 10ms
// =================================================

// ============== ★★★ 修正箇所 ★★★ ==============
// Dispatcher複数化に対応した、堅牢でスケーラブルなコア割り当て戦略

// ============== server setting ==============
// NUM_COREはビルドスクリプトによって上書きされるが、コンパイル時のためにデフォルト値を設定
#define NUM_CORE 16 // 例として16コアに設定

// --- バックグラウンドスレッドの定義 ---
// Dispatcherの数を定数で定義（この数を変更して実験する）
#define NUM_RO_DISPATCHERS 2

// RO Dispatcher以外のバックグラウンドスレッドの数
#define OTHER_BACKGROUND_THREADS 4
#define NUM_BACKGROUND_THREADS (OTHER_BACKGROUND_THREADS + NUM_RO_DISPATCHERS)

// バックグラウンドスレッドを高位コアから順に固定で割り当て
#define MULTIPLEXER_CORE (NUM_CORE - 1)
#define SEQUENCER_WRITER_CORE (NUM_CORE - 2)
#define SEQUENCER_READER_CORE (NUM_CORE - 3)
#define LOCK_MANAGER_CORE (NUM_CORE - 4)
// 複数のRO Dispatcherにコアを割り当てるためのマクロ
#define GET_RO_DISPATCHER_CORE(i) (NUM_CORE - 5 - (i))
// Note: MAIN_PROCESS_COREは通常multiplexerと兼用されるか、OSに任されるため、ここでは明示的に定義しない

// --- ワーカーの定義 ---
#define NUM_WORKERS_CORE (NUM_CORE - NUM_BACKGROUND_THREADS)
#define NUM_WORKERS (NUM_WORKERS_CORE)

// ワーカーを、番号の小さいコアから「偶数優先」で割り当てるためのマクロ
// これによりNUMAノードをまたぐメモリアクセスが減り、性能が向上する
#define GET_WORKER_CORE(thread_id)                \
    (((thread_id) < ((NUM_WORKERS_CORE + 1) / 2)) \
         ? ((thread_id) * 2)                      \
         : (((thread_id) - ((NUM_WORKERS_CORE + 1) / 2)) * 2 + 1))
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