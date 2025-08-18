#!/bin/bash

# ==============================================================================
# Calvin/PaxFlow CPUプロファイリング用スクリプト
# ==============================================================================

# --- 比較対象を設定 ('original' または 'proposed') ---
TARGET="proposed"

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
# ★★★ ここでスループットがスケールしなくなるスレッド数を指定してください ★★★
THREADS=4

# 測定時間
RUN_DURATION=10
# perfで記録する時間（測定時間より少し短くする）
PERF_DURATION=$((RUN_DURATION - 2))


if [ "$TARGET" == "original" ]; then
    NUM_BACKGROUND=4
    DEFINITIONS_FILE="definitions_original.hh"
    SOURCE_DIR="src_calvin"
else
    NUM_DISPATCHERS=1
    OTHER_BACKGROUND_THREADS=4
    NUM_BACKGROUND=$((OTHER_BACKGROUND_THREADS + NUM_DISPATCHERS))
    DEFINITIONS_FILE="definitions_proposed.hh"
    SOURCE_DIR="src_calvin_ext"
fi

# --- 基本設定 ---
SCRIPT_DIR=$(dirname "$0")
cd "$SCRIPT_DIR" || exit 1
LOG_FILE="/tmp/calvin_output.log"
PERF_DATA_FILE="perf.data" # 出力ファイル名

# ========================== 準備 ==========================
echo -e "\n======== Total Cores for Workers+Dispatchers: $THREADS ========"
if [ "$TARGET" == "original" ]; then
    NUM_WORKERS=$THREADS
else
    NUM_WORKERS=$((THREADS - NUM_DISPATCHERS))
fi
NUM_CORE=$((NUM_WORKERS + NUM_BACKGROUND))
echo "  - Configuration: Workers=$NUM_WORKERS, Background=$NUM_BACKGROUND, TotalCores=$NUM_CORE"

# --- ソースコードと定義ファイルの更新 ---
echo "  [1/3] ソースコードと定義ファイルを更新中..."
rm -rf src obj
cp -r "$SOURCE_DIR"/ src
cp "$DEFINITIONS_FILE" src/common/definitions.hh

sed -i -E "s/^#define[[:space:]]+NUM_CORE[[:space:]]+.*$/#define NUM_CORE $NUM_CORE/" src/common/definitions.hh
sed -i -E "s/^#define[[:space:]]+NUM_BACKGROUND_THREADS[[:space:]]+.*$/#define NUM_BACKGROUND_THREADS $NUM_BACKGROUND/" src/common/definitions.hh
sed -i -E "s/^#define[[:space:]]+NUM_WORKERS[[:space:]]+.*$/#define NUM_WORKERS $NUM_WORKERS/" src/common/definitions.hh
if [ "$TARGET" != "original" ]; then
    sed -i -E "s/^#define[[:space:]]+NUM_RO_DISPATCHERS[[:space:]]+.*$/#define NUM_RO_DISPATCHERS $NUM_DISPATCHERS/" src/common/definitions.hh
fi

# --- ビルド ---
echo "  [2/3] ビルドを実行中..."
cd src
make clean > /dev/null 2>&1
if ! make -j$(nproc); then
    echo "      - ❌ ビルド失敗。スキップします。"
    cd ..
    exit 1
fi
cd ..
echo "      - ビルド成功。"

# ========================== プロファイリング実行 ==========================
echo "  [3/3] プロファイリングを実行中..."

# 既存のperf.dataがあれば削除
rm -f "$PERF_DATA_FILE"

# データベースプロセスをバックグラウンドで開始
./bin/deployment/db 0 "$ARGUMENT" 0 > "$LOG_FILE" 2>&1 &
APP_PID=$!
sleep 1 # プロセスが完全に起動するのを少し待つ

# ★★★ エラーチェックの強化 ★★★
if ! ps -p $APP_PID > /dev/null; then
    echo "❌ データベースプロセスの起動に失敗しました。"
    echo "ログファイルを確認してください: $LOG_FILE"
    exit 1
fi
if ! [[ "$PERF_DURATION" -gt 0 ]]; then
    echo "❌ 測定時間の設定が不正です。"
    exit 1
fi
echo "  -> プロセスPIDは $APP_PID です。($PERF_DURATION 秒間プロファイリングします)"

# perf record を実行 (-g: コールグラフ取得)
# sudoの実行にパスワードが必要な場合があります
sudo perf record -p $APP_PID -g -- sleep "$PERF_DURATION"

# プロセスを停止
echo "  -> プロセスを停止します..."
kill -SIGINT "$APP_PID" 2>/dev/null
wait "$APP_PID" 2>/dev/null
sleep 1
if ps -p $APP_PID > /dev/null; then
   kill -SIGKILL "$APP_PID" 2>/dev/null
fi

# ========================== 後始末 ==========================
if [ -f "$PERF_DATA_FILE" ]; then
    echo -e "\n✅ プロファイリング完了。'$PERF_DATA_FILE' が生成されました。"
    echo "次のステップに進んで、Flame Graphを生成してください。"
else
    echo -e "\n❌ プロファイリングに失敗し、'$PERF_DATA_FILE' が生成されませんでした。"
fi