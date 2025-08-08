#!/bin/bash

# ==============================================================================
# Calvin系データベース性能測定スクリプト (改善版)
# ==============================================================================

# --- 引数処理 ---
# スクリプトの第1引数でベンチマークの種類を指定します (m: micro, t: TPCC, y: YCSB)
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
elif [ "$1" == "y" ]; then
    ARGUMENT="y"
else
    echo "エラー: 引数が無効です。'm' (microbenchmark), 't' (TPCC), 'y' (YCSB) のいずれかを使用してください。"
    exit 1
fi
echo "🧪 使用するベンチマーク: $ARGUMENT"

# ========================== 初期設定 ==========================
# --- 基本設定 ---
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1

# --- 実験パラメータ ---
THREAD_COUNTS=(1 2 4 8 16 32 48 64 72 80 91) # テストするワーカー・スレッド数のリスト
RUN_DURATION=10      # 1回あたりの実行時間 (秒)
NUM_RUNS=10           # 各スレッド数で試行する回数

# ----------- ★★★ 修正箇所 ★★★ -----------
# RO Dispatcher スレッドが追加されたため、バックグラウンドスレッド数を5に更新
NUM_BACKGROUND=5     # Background threads: Multiplexer, SequencerWriter, SequencerReader, LockManager, RODispatcher
# -----------------------------------------

# --- ファイル設定 ---
CONFIG_FILE="cygnus-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
OUTPUT_CSV="throughput_latency_summary_${ARGUMENT}.csv"
LOG_FILE="/tmp/calvin_output.log"

# --- 事前準備 ---
# 既存のログや失敗ログを削除
rm -f /tmp/failed_log_*.log
# 設定ファイルをバックアップ
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップしました: $BACKUP_CONFIG_FILE"
# 結果用CSVファイルのヘッダーを書き込み
echo "Threads,Average_Throughput(ops/sec),Average_Dispatcher_ms,Average_Queueing_ms,Average_Worker_ms" > "$OUTPUT_CSV"

# ========================== メインループ ==========================
for THREADS in "${THREAD_COUNTS[@]}"; do
    echo -e "\n======== スレッド数: $THREADS ========"

    # --- ビルド設定 ---
    NUM_WORKERS=$THREADS
    # 全体のコア数はワーカー数とバックグラウンドスレッド数の合計
    NUM_CORE=$((NUM_WORKERS + NUM_BACKGROUND))

    # --- ソースコードと定義ファイルの更新 ---
    echo "  [1/4] ソースコードと定義ファイルを更新中..."
    rm -rf src obj
    cp -r src_calvin_ext/ src
    cp definitions_throughput.hh src/common/definitions.hh
    echo "Compiling .proto files..."
    mkdir -p obj/proto  # ← これが必要！
    protoc -I=src/proto --cpp_out=obj/proto src/proto/message.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/txn.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc_args.proto

    # definitions.hh 内のマクロを sed で置換
    sed -i -E "s/^#define[[:space:]]+NUM_WORKERS[[:space:]]+.*$/#define NUM_WORKERS $NUM_WORKERS/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $NUM_CORE/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS $NUM_BACKGROUND/" src/common/definitions.hh
    
    # 見た目用に config ファイルも更新 (プログラムの動作に影響しない場合もある)
    awk -v cores="$NUM_WORKERS" 'BEGIN {FS=OFS=":"} /^node0=/ {$3=cores} 1' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"
    echo "      - NUM_WORKERS=$NUM_WORKERS, NUM_CORE=$NUM_CORE に設定しました。"

    # --- ビルド ---
    echo "  [2/4] ビルドを実行中..."
    cd src
    make clean > /dev/null 2>&1
    if make -j$(nproc) > /dev/null 2>&1; then
        echo "      - ビルド成功。"
    else
        echo "      - ❌ ビルド失敗 ($THREADS threads)。このスレッド数のテストをスキップします。"
        cd ..
        continue
    fi
    cd ..

    # ========================== 測定実行 ==========================
    echo "  [3/4] 測定を実行中 ($NUM_RUNS 回)..."
    TOTAL_THROUGHPUT=0
    TOTAL_DISPATCHER=0
    TOTAL_QUEUEING=0
    TOTAL_WORKER=0
    SUCCESSFUL_RUNS=0

    # 測定結果を格納する配列
    THROUGHPUT_ARRAY=()
    DISPATCHER_ARRAY=()
    QUEUEING_ARRAY=()
    WORKER_ARRAY=()

    for i in $(seq 1 $NUM_RUNS); do
        # データベースプログラムをバックグラウンドで起動
        ./bin/deployment/db 0 "$ARGUMENT" 0 > "$LOG_FILE" 2>&1 &
        APP_PID=$!

        # 指定時間待機
        sleep "$RUN_DURATION"

        # プロセスを終了させる
        kill -SIGINT "$APP_PID" 2>/dev/null
        sleep 2
        if ps -p $APP_PID > /dev/null; then
           kill -SIGKILL "$APP_PID" 2>/dev/null
        fi

        # --- 測定値抽出 ---
        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')
        DISPATCHER=$(grep -oP 'Dispatcher:\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')
        QUEUEING=$(grep -oP 'Queueing:\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')
        WORKER=$(grep -oP 'Worker:\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')

        if [ -z "$THROUGHPUT" ] || [ -z "$DISPATCHER" ] || [ -z "$QUEUEING" ] || [ -z "$WORKER" ]; then
            echo "    - 実行 $i/$NUM_RUNS: ⚠️ 測定値抽出失敗"
            FAILED_LOG_NAME="/tmp/failed_log_threads_${THREADS}_run_${i}.log"
            mv "$LOG_FILE" "$FAILED_LOG_NAME"
            echo "      (ログを ${FAILED_LOG_NAME} に保存しました)"
        else
            echo "    - 実行 $i/$NUM_RUNS: ✅ スループット: $THROUGHPUT ops/sec | Dispatcher: $DISPATCHER ms | Queueing: $QUEUEING ms | Worker: $WORKER ms"
            THROUGHPUT_ARRAY+=($THROUGHPUT)
            DISPATCHER_ARRAY+=($DISPATCHER)
            QUEUEING_ARRAY+=($QUEUEING)
            WORKER_ARRAY+=($WORKER)
            SUCCESSFUL_RUNS=$((SUCCESSFUL_RUNS + 1))
        fi
        rm -f "$LOG_FILE"
    done

    # ========================== 結果集計 ==========================
    echo "  [4/4] 結果を集計中..."
    if [ "$SUCCESSFUL_RUNS" -gt 1 ]; then
        # 最初の1回をスキップして、残りの平均を計算
        NUM_VALID_RUNS=$((SUCCESSFUL_RUNS - 1))
        
        TOTAL_THROUGHPUT=0
        TOTAL_DISPATCHER=0
        TOTAL_QUEUEING=0
        TOTAL_WORKER=0

        for ((j=1; j<SUCCESSFUL_RUNS; j++)); do
            TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + ${THROUGHPUT_ARRAY[$j]}}")
            TOTAL_DISPATCHER=$(awk "BEGIN {print $TOTAL_DISPATCHER + ${DISPATCHER_ARRAY[$j]}}")
            TOTAL_QUEUEING=$(awk "BEGIN {print $TOTAL_QUEUEING + ${QUEUEING_ARRAY[$j]}}")
            TOTAL_WORKER=$(awk "BEGIN {print $TOTAL_WORKER + ${WORKER_ARRAY[$j]}}")
        done
        
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $NUM_VALID_RUNS}")
        AVERAGE_DISPATCHER=$(awk "BEGIN {printf \"%.2f\", $TOTAL_DISPATCHER / $NUM_VALID_RUNS}")
        AVERAGE_QUEUEING=$(awk "BEGIN {printf \"%.2f\", $TOTAL_QUEUEING / $NUM_VALID_RUNS}")
        AVERAGE_WORKER=$(awk "BEGIN {printf \"%.2f\", $TOTAL_WORKER / $NUM_VALID_RUNS}")

        echo "✅ 平均スループット ($THREADS threads): $AVERAGE_THROUGHPUT ops/sec ($NUM_VALID_RUNS/$NUM_RUNS 成功, 最初の1回は除外)"
        echo "✅ 平均レイテンシ ($THREADS threads): Dispatcher: ${AVERAGE_DISPATCHER}ms, Queueing: ${AVERAGE_QUEUEING}ms, Worker: ${AVERAGE_WORKER}ms"
    else
        AVERAGE_THROUGHPUT="N/A"
        AVERAGE_DISPATCHER="N/A"
        AVERAGE_QUEUEING="N/A"
        AVERAGE_WORKER="N/A"
        echo "⚠️  $THREADS threads の実行が2回以上成功しなかったため、平均を計算できませんでした。"
    fi

    echo "$THREADS,$AVERAGE_THROUGHPUT,$AVERAGE_DISPATCHER,$AVERAGE_QUEUEING,$AVERAGE_WORKER" >> "$OUTPUT_CSV"
done

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo -e "\n🛠️  設定ファイルを元に戻しました。"
echo "🎉 全実験完了。結果は $OUTPUT_CSV に保存されました。"