#!/bin/bash

set -euo pipefail

LOG_DIR="删除节点"
NODES=(one two three four five)
REMOVE_NODE="five"
REMAINING_NODES=(one two three four)
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
  echo "Checking NFD status..."
  nfdc status || { echo "NFD failed to start"; exit 1; }
else
  echo "Warning: nfd-start not found, assuming NFD is already running."
fi

echo "Erasing NFD content store..."
nfdc cs erase /

echo "Setting sync strategy for /kua/sync/..."
nfdc strategy set /kua/sync/health $NFD_STRATEGY
nfdc strategy set /kua/sync/ $NFD_STRATEGY || true

export NDN_LOG="kua.*=DEBUG"

start_node() {
  local nodeName="$1"
  echo "Starting node $nodeName"
  ./build/bin/kua /kua /$nodeName >> "$LOG_DIR/$nodeName.log" 2>&1 &
}

echo "Starting initial 5 nodes..."
for name in "${NODES[@]}"; do
  start_node "$name"
  sleep 1
done

sleep 12

echo "Inserting pre-failure data (covering bucket 1 and bucket 3)..."
TEST_DATA=(
  "/gamma2:prefail bucket1 item1"
  "/delta4:prefail bucket1 item2"
  "/gamma7:prefail bucket1 item3"
  "/alpha2:prefail bucket3 item1"
  "/beta2:prefail bucket3 item2"
  "/alpha7:prefail bucket3 item3"
  "/delta6:prefail bucket1 item4"
  "/gamma8:prefail bucket3 item4"
)

for data in "${TEST_DATA[@]}"; do
  IFS=':' read -r prefix content <<< "$data"
  echo "Putting $prefix"
  echo "$content" | ./build/bin/kua-client put "$prefix" >> "$LOG_DIR/client-put.log" 2>&1
  sleep 1
done

sleep 6

echo "Simulating node failure: stopping $REMOVE_NODE"
sudo pkill -f "./build/bin/kua /kua /$REMOVE_NODE" || true
sleep 1

# Give membership change + reassignment + migration enough time.
echo "Waiting for reassignment and migration after node removal..."
sleep 25

echo "Verifying all data remain accessible after node removal..."
SUCCESS_COUNT=0
TOTAL_TESTS=${#TEST_DATA[@]}

for data in "${TEST_DATA[@]}"; do
  IFS=':' read -r prefix content <<< "$data"
  outFile="$LOG_DIR/client-get-${prefix##*/}.out"
  ./build/bin/kua-client get "$prefix" > "$outFile" 2>&1 || true

  if grep -q "$content" "$outFile"; then
    echo "SUCCESS: $prefix accessible"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "FAIL: $prefix missing after node removal"
    echo "--- output of $prefix ---"
    cat "$outFile"
  fi
done

if [ "$SUCCESS_COUNT" -ne "$TOTAL_TESTS" ]; then
  echo "FAIL: only $SUCCESS_COUNT/$TOTAL_TESTS items accessible"
  exit 1
fi

echo "SUCCESS: all $TOTAL_TESTS items accessible after node removal"

echo "Checking migration logs on remaining nodes..."
if grep -qE "开始迁移 bucket|迁移 bucket|发送迁移请求|接收数据并存储成功" "$LOG_DIR"/{one,two,three,four}.log; then
  echo "SUCCESS: migration/receive logs detected on remaining nodes"
  grep -E "开始迁移 bucket|迁移 bucket|发送迁移请求|接收数据并存储成功" "$LOG_DIR"/{one,two,three,four}.log > "$LOG_DIR/migration-summary.log" || true
  echo "Migration summary: $LOG_DIR/migration-summary.log"
else
  echo "WARN: no explicit migration log found; data still remained readable"
fi

echo "Test complete. Logs written to $LOG_DIR"
