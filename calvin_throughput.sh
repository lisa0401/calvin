#!/bin/bash

# ========================== 引数処理 ==========================
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
elif [ "$1" == "y" ]; then
    ARGUMENT="y"
else
    echo "Invalid argument. Use 'm' (microbenchmark), 't' (TPCC), or 'y' (YCSB)."
    exit 1
fi
echo "🧪 使用するベンチマーク: $ARGUMENT"

# ========================== 初期設定 ==========================
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1

CONFIG_FILE="cygnus-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップ: $BACKUP_CONFIG_FILE"

OUTPUT_CSV="throughput_summary_${ARGUMENT}.csv"
echo "Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"

THREAD_COUNTS=(1 2 4 8 16 32 48 64 72 90) # テストするワーカー・スレッド数のリスト
RUN_DURATION=10
NUM_RUNS=10
NUM_BACKGROUND=5

# ========================== メインループ ==========================
for THREADS in "${THREAD_COUNTS[@]}"; do
    echo "======== スレッド数: $THREADS ========"

    NUM_WORKERS=$THREADS
    NUM_CORE=$((NUM_WORKERS + NUM_BACKGROUND))

    # ソース復元と定義ファイル更新
    rm -rf src obj
    cp -r src_calvin/ src
    cp definition_throughput.hh src/common/definitions.hh

    echo "Compiling .proto files..."
    mkdir -p obj/proto  # ← これが必要！
    protoc -I=src/proto --cpp_out=obj/proto src/proto/message.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/txn.proto
    protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc_args.proto

    sed -i -E "s/^#define[[:space:]]+NUM_WORKERS[[:space:]]+.*$/#define NUM_WORKERS $NUM_WORKERS/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $NUM_CORE/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS $NUM_BACKGROUND/" src/common/definitions.hh
    # 見た目用に config ファイルも更新
    awk -v cores="$NUM_WORKERS" 'BEGIN {FS=OFS=":"} /^node0=/ {$3=cores} 1' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"

    # ビルド
    cd src
    make clean
    make -j$(nproc)
    BUILD_STATUS=$?
    cd ..
    if [ $BUILD_STATUS -ne 0 ]; then
        echo "❌ ビルド失敗 ($THREADS threads)。スキップします。"
        continue
    fi

    # ========================== 実行 ==========================
    TOTAL_THROUGHPUT=0
    SUCCESSFUL_RUNS=0

    for i in $(seq 1 $NUM_RUNS); do
        echo "  ▶ 実行 $i/$NUM_RUNS..."
        ./bin/deployment/db 0 "$ARGUMENT" 0 > /tmp/calvin_output.log 2>&1 &
        APP_PID=$!
        sleep "$RUN_DURATION"
        kill -SIGINT "$APP_PID" 2>/dev/null
        sleep 1
        kill -SIGKILL "$APP_PID" 2>/dev/null

        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+\s*txns/sec' /tmp/calvin_output.log | tail -n 1 | awk '{print $1}')

        if [ -z "$THROUGHPUT" ]; then
            echo "    ⚠️  実行 $i: スループット抽出失敗"
            cat /tmp/calvin_output.log
        else
            echo "    ✅ スループット: $THROUGHPUT ops/sec"
            TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + $THROUGHPUT}")
            SUCCESSFUL_RUNS=$((SUCCESSFUL_RUNS + 1))
        fi
        rm -f /tmp/calvin_output.log
    done

    # ========================== 結果集計 ==========================
    if [ "$SUCCESSFUL_RUNS" -gt 0 ]; then
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $SUCCESSFUL_RUNS}")
        echo "✅ 平均スループット（$THREADS threads）: $AVERAGE_THROUGHPUT ops/sec ($SUCCESSFUL_RUNS/$NUM_RUNS 成功)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  $THREADS threads の全実行に失敗しました。"
    fi

    echo "$THREADS,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
done

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo "🛠️ 設定ファイルを元に戻しました。"
echo "✅ 全実験完了。結果ファイル: $OUTPUT_CSV"
