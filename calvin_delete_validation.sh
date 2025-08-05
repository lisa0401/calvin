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
    echo "Defaulting to 'm' (microbenchmark)."
    ARGUMENT="y"
fi

# 必要であればライブラリパスを有効化します
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/protobuf/src/.libs
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/zookeeper/.libs
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/zeromq/src/.libs

# 既存のビルド成果物とソースをクリーンアップします
echo "Cleaning up old directories..."
rm -rf src obj

# OCC版のソースコードをビルド用のディレクトリにコピーします
echo "Copying OCC source files..."
cp -r src_calvin_read_only/ src
cp definitions.hh src/common/definitions.hh

# ソースコードをビルドします
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

