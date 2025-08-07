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
THREAD_COUNTS=(1 2 4 8 16 32 48 64 72 80 88 92) # テストするワーカー・スレッド数のリスト
RUN_DURATION=10      # 1回あたりの実行時間 (秒)
NUM_RUNS=5           # 各スレッド数で試行する回数

# ----------- ★★★ 修正箇所 ★★★ -----------
# RO Dispatcher スレッドが追加されたため、バックグラウンドスレッド数を5に更新
NUM_BACKGROUND=5     # Background threads: Multiplexer, SequencerWriter, SequencerReader, LockManager, RODispatcher
# -----------------------------------------

# --- ファイル設定 ---
CONFIG_FILE="cygnus-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
OUTPUT_CSV="throughput_ext_summary_${ARGUMENT}.csv"
LOG_FILE="/tmp/calvin_output.log"

# --- 事前準備 ---
# 既存のログや失敗ログを削除
rm -f /tmp/failed_log_*.log
# 設定ファイルをバックアップ
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップしました: $BACKUP_CONFIG_FILE"
# 結果用CSVファイルのヘッダーを書き込み
echo "Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"

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
    SUCCESSFUL_RUNS=0

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

        # --- スループット抽出 ---
        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')

        if [ -z "$THROUGHPUT" ]; then
            echo "    - 実行 $i/$NUM_RUNS: ⚠️ スループット抽出失敗"
            FAILED_LOG_NAME="/tmp/failed_log_threads_${THREADS}_run_${i}.log"
            mv "$LOG_FILE" "$FAILED_LOG_NAME"
            echo "      (ログを ${FAILED_LOG_NAME} に保存しました)"
        else
            echo "    - 実行 $i/$NUM_RUNS: ✅ スループット: $THROUGHPUT ops/sec"
            TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + $THROUGHPUT}")
            SUCCESSFUL_RUNS=$((SUCCESSFUL_RUNS + 1))
        fi
        rm -f "$LOG_FILE"
    done

    # ========================== 結果集計 ==========================
    echo "  [4/4] 結果を集計中..."
    if [ "$SUCCESSFUL_RUNS" -gt 0 ]; then
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $SUCCESSFUL_RUNS}")
        echo "✅ 平均スループット ($THREADS threads): $AVERAGE_THROUGHPUT ops/sec ($SUCCESSFUL_RUNS/$NUM_RUNS 成功)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  $THREADS threads の全実行に失敗しました。"
    fi

    echo "$THREADS,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
done

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo -e "\n🛠️  設定ファイルを元に戻しました。"
echo "🎉 全実験完了。結果は $OUTPUT_CSV に保存されました。"
