#!/bin/bash

set -euo pipefail

LOG_DIR="KV阶段1"
NODES=(one two three four)
NFD_STRATEGY="/localhost/nfd/strategy/multicast"

if [ -d "$LOG_DIR" ]; then
  echo "Cleaning old log files in $LOG_DIR"
  rm -rf "$LOG_DIR"/* "$LOG_DIR"/.[!.]* "$LOG_DIR"/..?* 2>/dev/null || true
else
  mkdir -p "$LOG_DIR"
fi

echo "Cleaning old kua processes..."
sudo pkill -f './build/bin/kua' || true
sudo pkill -f './build/bin/kua-client' || true
sudo pkill -f nfd || true
sleep 1

if command -v nfd-start >/dev/null 2>&1; then
  echo "Starting NFD..."
  nfd-start >/dev/null 2>&1 || true
  sleep 3
  nfdc status || { echo "NFD failed to start"; exit 1; }
fi

echo "Erasing NFD content store..."
nfdc cs erase /

echo "Setting sync strategy..."
nfdc strategy set /kua/sync/health $NFD_STRATEGY
nfdc strategy set /kua/sync/ $NFD_STRATEGY || true

export NDN_LOG="kua.*=DEBUG"

start_node() {
  local nodeName="$1"
  echo "Starting node $nodeName"
  ./build/bin/kua /kua /$nodeName >> "$LOG_DIR/$nodeName.log" 2>&1 &
}

echo "Starting nodes..."
for n in "${NODES[@]}"; do
  start_node "$n"
  sleep 1
done

sleep 12

echo "Test 1: KV put/get"
./build/bin/kua-client kv-put userA v1 > "$LOG_DIR/kv-put-userA-v1.out" 2> "$LOG_DIR/kv-put-userA-v1.err"
sleep 1
./build/bin/kua-client kv-get userA > "$LOG_DIR/kv-get-userA-v1.out" 2> "$LOG_DIR/kv-get-userA-v1.err"
if ! grep -qx "v1" "$LOG_DIR/kv-get-userA-v1.out"; then
  echo "FAIL: expected userA=v1"
  cat "$LOG_DIR/kv-get-userA-v1.out"
  exit 1
fi

echo "Test 2: KV update should return latest value"
./build/bin/kua-client kv-put userA v2 > "$LOG_DIR/kv-put-userA-v2.out" 2> "$LOG_DIR/kv-put-userA-v2.err"
sleep 1
./build/bin/kua-client kv-get userA > "$LOG_DIR/kv-get-userA-v2.out" 2> "$LOG_DIR/kv-get-userA-v2.err"
if ! grep -qx "v2" "$LOG_DIR/kv-get-userA-v2.out"; then
  echo "FAIL: expected userA=v2 after update"
  cat "$LOG_DIR/kv-get-userA-v2.out"
  exit 1
fi

echo "Test 3: Add another key and list all"
./build/bin/kua-client kv-put userB b1 > "$LOG_DIR/kv-put-userB-b1.out" 2> "$LOG_DIR/kv-put-userB-b1.err"
sleep 1
./build/bin/kua-client kv-list > "$LOG_DIR/kv-list-all.out" 2> "$LOG_DIR/kv-list-all.err"

if ! grep -q $'^userA\t' "$LOG_DIR/kv-list-all.out"; then
  echo "FAIL: kv-list missing userA"
  cat "$LOG_DIR/kv-list-all.out"
  exit 1
fi

if ! grep -q $'^userB\t' "$LOG_DIR/kv-list-all.out"; then
  echo "FAIL: kv-list missing userB"
  cat "$LOG_DIR/kv-list-all.out"
  exit 1
fi

echo "SUCCESS: phase-1 KV tests passed"
echo "Logs in $LOG_DIR"
