#!/bin/bash

# 引数の処理
if [ "$1" == "m" ]; then
    ARGUMENT="m"
elif [ "$1" == "t" ]; then
    ARGUMENT="t"
else
    ARGUMENT="t"  # デフォルト値を"m"とする
fi

# 必要であれば LD_LIBRARY_PATH を有効化
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/protobuf/src/.libs
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/zookeeper/.libs
# export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/calvin/ext/zeromq/src/.libs

# クリーンアップと OCC バージョンのソースコピー
rm -rf src obj
cp -r src_calvin_ext/ src
cp definitions.hh src/common/definitions.hh

# ビルド
cd src
make clean
make -j
cd ../

# 実行（0番ノード、引数あり）
bin/deployment/db 0 "$ARGUMENT" 0
