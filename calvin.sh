#!/bin/bash

# スクリプトの引数を処理して、実行するベンチマークを決定します
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
elif [ "$1" == "y" ]; then
    ARGUMENT="y"
else
    echo "Invalid argument. Use 'm' for microbenchmark, 't' for TPC-C, or 'y' for YCSB."
    echo "Defaulting to 'y' (YCSB)."
    ARGUMENT="y"
fi

# 既存のビルド成果物とソースをクリーンアップします
echo "Cleaning up old directories..."
rm -rf src obj bin db db/storage db/checkpoints

# OCC版のソースコードをビルド用のディレクトリにコピーします
echo "Copying OCC source files..."
cp -r src_calvin/ src
cp definitions.hh src/common/definitions.hh

# =========================
# .proto ファイルのコンパイル
# =========================
echo "Compiling .proto files..."
mkdir -p obj/proto  # ← これが必要！
protoc -I=src/proto --cpp_out=obj/proto src/proto/message.proto
protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc.proto
protoc -I=src/proto --cpp_out=obj/proto src/proto/txn.proto
protoc -I=src/proto --cpp_out=obj/proto src/proto/tpcc_args.proto


# =========================
# ソースコードをビルドします
# =========================
echo "Building the source code..."
cd src
make clean
make -j
cd ../

# ビルドが成功したか確認します
if [ ! -f "bin/deployment/db" ]; then
    echo "Build failed. Exiting."
    exit 1
fi

# ベンチマークプログラムを実行します
# 引数: <node-id> <application-type> <percent_mp>
echo "Starting benchmark with argument: $ARGUMENT"
bin/deployment/db 0 "$ARGUMENT" 0


