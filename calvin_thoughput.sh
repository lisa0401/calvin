#!/bin/bash

# スクリプトの引数処理
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
else
    ARGUMENT="t"
fi
echo "使用する引数: $ARGUMENT"

# パス設定
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1

# 設定ファイル名とバックアップ
CONFIG_FILE="deploy-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "設定ファイルをバックアップしました: $BACKUP_CONFIG_FILE"

# スループット結果ファイル
OUTPUT_CSV="throughput_summary_${ARGUMENT}.csv"
echo "Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"

# テスト設定
THREAD_COUNTS=(1 2 4 8 16)
RUN_DURATION=10
NUM_RUNS=20

for THREADS in "${THREAD_COUNTS[@]}"; do
    echo "======== スレッド数: $THREADS ========"

    # src をリセットしてクリーンなビルド状態にする
    rm -rf src obj
    cp -r src_calvin/ src
    cp definitions.hh src/common/definitions.hh

    # NUM_CORE を THREADS に書き換え、NUM_BACKGROUND_THREADS を 0 に強制
    # スレッド数に応じて NUM_CORE を定義し直し
    sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $THREADS/" src/common/definitions.hh

# NUM_BACKGROUND_THREADS を必ず 0 にする（定数でも式でもOK）
    sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS 0/" src/common/definitions.hh

    # deploy-run.conf のコア数を同期（見た目の整合性）
    awk -v cores="$THREADS" 'BEGIN {FS=OFS=":"} /^node0=/ {$3=cores} 1' "$CONFIG_FILE" > "${CONFIG_FILE}.tmp" && mv "${CONFIG_FILE}.tmp" "$CONFIG_FILE"

    # ビルド実行
    cd src
    make clean
    make -j$(nproc)
    BUILD_STATUS=$?
    cd ..
    if [ $BUILD_STATUS -ne 0 ]; then
        echo "❌ ビルド失敗 ($THREADS threads)。スキップします。"
        continue
    fi

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

    if [ "$SUCCESSFUL_RUNS" -gt 0 ]; then
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $SUCCESSFUL_RUNS}")
        echo "✅ 平均スループット（$THREADS threads）: $AVERAGE_THROUGHPUT ops/sec ($SUCCESSFUL_RUNS/$NUM_RUNS 成功)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  $THREADS threads の全実行に失敗しました。"
    fi

    echo "$THREADS,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
done

# 元の config を復元
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo "設定ファイルを元に戻しました。"

echo "✅ 実行完了。結果: $OUTPUT_CSV"
