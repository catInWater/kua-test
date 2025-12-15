#!/bin/bash

# 定义日志目录
LOG_DIR="log"

# 创建日志目录（如果不存在）
mkdir -p $LOG_DIR

# 清理旧日志
rm -f $LOG_DIR/*.log

# 设置日志级别
export NDN_LOG="kua.*=DEBUG"

# 清理旧进程和NFD缓存
echo "Cleaning up old processes and NFD cache..."
sudo pkill -f kua
sudo pkill -f nfd
sleep 1
nfd-start
sleep 2
nfdc cs erase /

# 设置默认路由策略为 best-route（更稳定可靠）
echo "Setting NFD strategy to best-route..."
nfdc strategy set / /localhost/nfd/strategy/best-route

# 启动 Master 节点
echo "Starting master node..."
./build/bin/kua-master /kua /master 2>&1 | tee $LOG_DIR/master.log &

# 给 Master 一些初始化时间
sleep 5

# 定义启动普通节点的函数，增加更长的间隔
startnode () {
    local name=$1
    echo "Starting node $name..."
    ./build/bin/kua /kua /$name >> $LOG_DIR/$name.log 2>&1 &
    # 增加节点启动间隔，确保每个节点都能被平稳初始化
    sleep 2
}

# 逐个启动普通节点
startnode one
startnode two
startnode three
startnode four
startnode five
startnode six
startnode seven
startnode eight

echo "All nodes started. Check logs in $LOG_DIR directory."