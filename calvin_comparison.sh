#!/bin/bash

# ==============================================================================
# Calvin 比較実験用スクリプト (72コアサーバー向け・スレッド数ごと・公平比較版)
# ==============================================================================

# --- 比較対象を設定 ('original' または 'proposed') ---
TARGET="proposed" # ここを 'original' に変えて元カルバンを測定

# --- 引数処理 ---
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
elif [ "$1" == "y" ]; then
    ARGUMENT="y"
else
    echo "エラー: 引数が無効です。'm', 't', 'y' のいずれかを使用してください。"
    exit 1
fi
echo "🧪 使用するベンチマーク: $ARGUMENT"
echo "🎯 測定対象: $TARGET"

# ========================== 実験パラメータ ==========================

# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ ここを修正：提案手法の追加スレッド設定 ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
NUM_DISPATCHERS_PROPOSED=1
SNAPSHOT_THREAD_CORE_PROPOSED=1 # 提案手法のSnapshotスレッド数
BASE_BACKGROUND_THREADS=4     # Dispatcher/Snapshot以外のBackgroundスレッド数

# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ ここを修正：比較する総アプリスレッド数のリスト ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# (例: 34スレッド = original: 34W, proposed: 32W + 1D + 1S)
THREAD_COUNTS=(3 8 16 32 64)


if [ "$TARGET" == "original" ]; then
    # 従来手法
    NUM_DISPATCHERS=0
    SNAPSHOT_THREAD_CORE=0
    NUM_BACKGROUND=$BASE_BACKGROUND_THREADS # (4)
    
    DEFINITIONS_FILE="definitions_original.hh"
    SOURCE_DIR="src_calvin" # 元カルバンのソースディレクトリ
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_original_comp.csv"
else
    # 提案手法: ベースワーカー数 + 専用スレッド
    NUM_DISPATCHERS=$NUM_DISPATCHERS_PROPOSED
    SNAPSHOT_THREAD_CORE=$SNAPSHOT_THREAD_CORE_PROPOSED
    NUM_BACKGROUND=$((BASE_BACKGROUND_THREADS + NUM_DISPATCHERS + SNAPSHOT_THREAD_CORE)) # (4 + 1 + 1 = 6)
    
    DEFINITIONS_FILE="definitions_proposed.hh"
    SOURCE_DIR="src_calvin_ext" # 提案手法のソースディレクトリ
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_proposed_d${NUM_DISPATCHERS}_s${SNAPSHOT_THREAD_CORE}.csv"
fi

echo "--- 実行構成 ($TARGET) ---"
echo "  NUM_DISPATCHERS: $NUM_DISPATCHERS"
echo "  SNAPSHOT_THREAD_CORE: $SNAPSHOT_THREAD_CORE"
echo "  BASE_BACKGROUND_THREADS: $BASE_BACKGROUND_THREADS"
echo "  TOTAL NUM_BACKGROUND: $NUM_BACKGROUND"
echo "--------------------------"


# --- 測定回数 ---
RUN_DURATION=10
NUM_RUNS=4
WARMUP_RUNS=0

# --- 基本設定 ---
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1
CONFIG_FILE="deploy-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
LOG_FILE="/tmp/calvin_output.log"

# --- 事前準備 ---
rm -f /tmp/failed_log_*.log
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップ: $BACKUP_CONFIG_FILE"
echo "Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"
echo "📈 結果出力先: $OUTPUT_CSV"

# ========================== メインループ ==========================
for THREADS_DISPLAY in "${THREAD_COUNTS[@]}"; do
    echo -e "\n======== Total App Cores: $THREADS_DISPLAY ========"

    # --- 公平な比較のためのワーカー数調整 ---
    if [ "$TARGET" == "original" ]; then
        NUM_WORKERS=$THREADS_DISPLAY
    else
        NUM_WORKERS=$((THREADS_DISPLAY - NUM_DISPATCHERS - SNAPSHOT_THREAD_CORE))
        if [ "$NUM_WORKERS" -lt 1 ]; then
            echo "ワーカー数が1未満($NUM_WORKERS)になるためスキップします。"
            continue
        fi
    fi
    
    NUM_CORE=$((NUM_WORKERS + NUM_BACKGROUND))
    
    if [ "$NUM_CORE" -gt 72 ]; then
        echo "合計コア数($NUM_CORE)がサーバーの上限(72)を超えるためスキップします。"
        continue
    fi
    
    echo "  - Configuration: Workers=$NUM_WORKERS, Background=$NUM_BACKGROUND, TotalCores=$NUM_CORE"

    # --- ソースコードと定義ファイルの更新 ---
    echo "  [1/4] ソースコードと定義ファイルを更新中..."
    rm -rf src obj
    cp -r "$SOURCE_DIR"/ src
    cp "$DEFINITIONS_FILE" src/common/definitions.hh

    # sedで各種マクロを置換
    sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $NUM_CORE/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS $NUM_BACKGROUND/" src/common/definitions.hh
    sed -i -E "s/^#define[[:space:]]+NUM_WORKERS[[:space:]]+.*$/#define NUM_WORKERS $NUM_WORKERS/" src/common/definitions.hh
    if [ "$TARGET" != "original" ]; then
        sed -i -E "s/^#define[[:space:]]+NUM_RO_DISPATCHERS[[:space:]]+.*$/#define NUM_RO_DISPATCHERS $NUM_DISPATCHERS/" src/common/definitions.hh
        sed -i -E "s/^#define[[:space:]]+SNAPSHOT_THREAD_CORE[[:space:]]+.*$/#define SNAPSHOT_THREAD_CORE $SNAPSHOT_THREAD_CORE/" src/common/definitions.hh
    fi
    
    # --- ビルド ---
    echo "  [2/4] ビルドを実行中..."
    cd src
    make clean > /dev/null 21
    if make -j$(nproc); then
        echo "      - ビルド成功。"
    else
        echo "      - ❌ ビルド失敗 (Workers=$NUM_WORKERS)。スキップします。"
        cd ..
        continue
    fi
    cd ..

    # ========================== 測定実行 ==========================
    echo "  [3/4] 測定を実行中 ($NUM_RUNS 回)..."
    THROUGHPUT_ARRAY=()

    for i in $(seq 1 $NUM_RUNS); do
        # Skew引数は 0 (デフォルト) を使用
        ./bin/deployment/db 0 "$ARGUMENT" 0 > "$LOG_FILE" 2>&1 &
        APP_PID=$!
        sleep "$RUN_DURATION"
        kill -SIGINT "$APP_PID" 2>/dev/null
        wait "$APP_PID" 2>/dev/null
        sleep 1
        if ps -p $APP_PID > /dev/null; then
           kill -SIGKILL "$APP_PID" 2>/dev/null
        fi

        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')

        if [ "$i" -le "$WARMUP_RUNS" ]; then
            echo "    - ウォームアップ実行 $i/$NUM_RUNS: 🚀 完了 (結果は破棄)"
        elif [ -z "$THROUGHPUT" ]; then
            echo "    - 測定実行 $i/$NUM_RUNS: ⚠️ スループット抽出失敗"
            FAILED_LOG_NAME="/tmp/failed_log_threads_${THREADS_DISPLAY}_run_${i}.log"
            mv "$LOG_FILE" "$FAILED_LOG_NAME"
            echo "      (ログを ${FAILED_LOG_NAME} に保存しました)"
        else
            echo "    - 測定実行 $i/$NUM_RUNS: ✅ スループット: $THROUGHPUT ops/sec"
            THROUGHPUT_ARRAY+=($THROUGHPUT)
        fi
        rm -f "$LOG_FILE"
    done

    # ========================== 結果集計 ==========================
    echo "  [4/4] 結果を集計中..."
    NUM_VALID_RUNS=${#THROUGHPUT_ARRAY[@]}
    if [ "$NUM_VALID_RUNS" -gt 0 ]; then
        TOTAL_THROUGHPUT=0
        for t in "${THROUGHPUT_ARRAY[@]}"; do
            TOTAL_THROUGHPUT=$(awk "BEGIN {print $TOTAL_THROUGHPUT + $t}")
        done
        
        AVERAGE_THROUGHPUT=$(awk "BEGIN {printf \"%.2f\", $TOTAL_THROUGHPUT / $NUM_VALID_RUNS}")
        echo "✅ 平均スループット (Total App Cores=$THREADS_DISPLAY): $AVERAGE_THROUGHPUT ops/sec ($NUM_VALID_RUNS 回の有効実行から算出)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  Total App Cores=$THREADS_DISPLAY の有効な実行がなかったため、平均を計算できませんでした。"
    fi
    
    # CSVには、比較スレッド総数、平均スループットを記録
    echo "$THREADS_DISPLAY,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
done

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo -e "\n🛠️  設定ファイルを元に戻しました。"
echo "🎉 全実験完了。結果は $OUTPUT_CSV に保存されました。"