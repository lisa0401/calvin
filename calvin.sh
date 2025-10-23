#!/bin/bash

# ==============================================================================
# Calvin 比較実験用スクリプト (72コアサーバー向け・W32固定・Skew対応版)
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
# ★★★ ここを修正：Skewレベルのリスト ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
SKEW_LEVELS=(0.0 0.2 0.4 0.6 0.8 1.0)

# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ ここを修正：基本となるワーカースレッド数 ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
BASE_WORKERS=32

# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
# ★★★ ここを修正：提案手法の追加スレッド設定 ★★★
# ★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★★
NUM_DISPATCHERS_PROPOSED=1
SNAPSHOT_THREAD_CORE_PROPOSED=1 # 提案手法のSnapshotスレッド数（仮に1と設定）
BASE_BACKGROUND_THREADS=4     # Dispatcher/Snapshot以外のBackgroundスレッド数

# --- 測定回数 ---
RUN_DURATION=10
NUM_RUNS=60
WARMUP_RUNS=10

# --- 基本設定 ---
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1
CONFIG_FILE="deploy-run.conf"
BACKUP_CONFIG_FILE="${CONFIG_FILE}.bak"
LOG_FILE="/tmp/calvin_output.log"

# --- 事前準備 (グローバル) ---
rm -f /tmp/failed_log_*.log
cp "$CONFIG_FILE" "$BACKUP_CONFIG_FILE"
echo "✅ 設定ファイルをバックアップ: $BACKUP_CONFIG_FILE"

# ========================== 単一CSVファイルの設定 ==========================

# --- スレッド数とBackground数の計算 ---
if [ "$TARGET" == "original" ]; then
    # 従来手法: 提案手法のオーバーヘッド分をワーカー数に加算
    NUM_DISPATCHERS=0
    SNAPSHOT_THREAD_CORE=0
    NUM_WORKERS=$((BASE_WORKERS + NUM_DISPATCHERS_PROPOSED + SNAPSHOT_THREAD_CORE_PROPOSED))
    NUM_BACKGROUND=$BASE_BACKGROUND_THREADS
    
    DEFINITIONS_FILE="definitions_original.hh"
    SOURCE_DIR="src_calvin" # 元カルバンのソースディレクトリ
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_original_w_comp${BASE_WORKERS}_all_skews.csv"
else
    # 提案手法: ベースワーカー数 + 専用スレッド
    NUM_DISPATCHERS=$NUM_DISPATCHERS_PROPOSED
    SNAPSHOT_THREAD_CORE=$SNAPSHOT_THREAD_CORE_PROPOSED
    NUM_WORKERS=$BASE_WORKERS
    NUM_BACKGROUND=$((BASE_BACKGROUND_THREADS + NUM_DISPATCHERS + SNAPSHOT_THREAD_CORE))
    
    DEFINITIONS_FILE="definitions_proposed.hh"
    SOURCE_DIR="src_calvin_ext" # 提案手法のソースディレクトリ
    OUTPUT_CSV="throughput_summary_${ARGUMENT}_proposed_d${NUM_DISPATCHERS}_s${SNAPSHOT_THREAD_CORE}_w${BASE_WORKERS}_all_skews.csv"
fi

echo "--- 実行構成 ($TARGET) ---"
echo "  NUM_WORKERS: $NUM_WORKERS"
echo "  NUM_DISPATCHERS: $NUM_DISPATCHERS"
echo "  SNAPSHOT_THREAD_CORE: $SNAPSHOT_THREAD_CORE"
echo "  NUM_BACKGROUND: $NUM_BACKGROUND"
echo "--------------------------"


# --- CSVファイル初期化 (ヘッダー書き込み) ---
echo "Skew,Threads,Average_Throughput(ops/sec)" > "$OUTPUT_CSV"
echo "📈 全ての結果出力先: $OUTPUT_CSV"


# ========================== メインループ (Skew) ==========================
for SKEW in "${SKEW_LEVELS[@]}"; do
    echo -e "\n\n############################################################"
    echo "############ SKEW レベル: $SKEW ############"
    echo "############################################################"

    # ========================== 測定実行 (固定スレッド) ==========================
    
    # --- ワーカースレッド数はループ外で設定済み ---
    
    if [ "$TARGET" == "original" ]; then
        # 'original' の場合、比較対象の総アプリスレッドは $NUM_WORKERS
        THREADS_DISPLAY=$NUM_WORKERS
    else
        # 'proposed' の場合、比較対象の総アプリスレッドは ワーカー+ディスパッチャ+スナップショット
        THREADS_DISPLAY=$((NUM_WORKERS + NUM_DISPATCHERS + SNAPSHOT_THREAD_CORE))
    fi
    
    echo -e "\n======== Total App Cores: $THREADS_DISPLAY (Workers=$NUM_WORKERS) (Skew: $SKEW) ========"
    
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
    make clean > /dev/null 2>&1
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
    
        # ★★★ ここを変更 ★★★
        # dbコマンドの3番目の引数をSKEW変数に変更
        # ./bin/deployment/db <node_id> <benchmark_type> <skew_factor>
        ./bin/deployment/db 0 "$ARGUMENT" "$SKEW" > "$LOG_FILE" 2>&1 &
        APP_PID=$!
        
        sleep "$RUN_DURATION"
        kill -SIGINT "$APP_PID" 2>/dev/null
        wait "$APP_PID" 2>/dev/null
        sleep 1
        if ps -p $APP_PID > /dev/null; then
           kill -SIGKILL "$APP_PID" 2>/dev/null
        fi

        # スループットを抽出
        THROUGHPUT=$(grep -oP 'Completed\s*\K[0-9.]+' "$LOG_FILE" | tail -n 5 | head -n 1 | awk '{print $1}')

        if [ "$i" -le "$WARMUP_RUNS" ]; then
            echo "    - ウォームアップ実行 $i/$NUM_RUNS: 🚀 完了 (結果は破棄)"
        elif [ -z "$THROUGHPUT" ]; then
            echo "    - 測定実行 $i/$NUM_RUNS: ⚠️ スループット抽出失敗"
            FAILED_LOG_NAME="/tmp/failed_log_skew_${SKEW}_threads_${THREADS_DISPLAY}_run_${i}.log"
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
        echo "✅ 平均スループット (Total App Cores=$THREADS_DISPLAY, $SKEW skew): $AVERAGE_THROUGHPUT ops/sec ($NUM_VALID_RUNS 回の有効実行から算出)"
    else
        AVERAGE_THROUGHPUT="N/A"
        echo "⚠️  Total App Cores=$THREADS_DISPLAY ($SKEW skew) の有効な実行がなかったため、平均を計算できませんでした。"
    fi
    
    # CSVには、Skewレベル、比較スレッド総数、平均スループットを記録
    echo "$SKEW,$THREADS_DISPLAY,$AVERAGE_THROUGHPUT" >> "$OUTPUT_CSV"
    #
    # ★★★ Threadループを削除したため、'done' は不要 ★★★
    #

done # Skewループの終わり

# ========================== 後始末 ==========================
mv "$BACKUP_CONFIG_FILE" "$CONFIG_FILE"
echo -e "\n🛠️  設定ファイルを元に戻しました。"
echo "🎉 全実験完了。結果は $OUTPUT_CSV に保存されました。"