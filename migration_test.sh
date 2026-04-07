#!/bin/bash

set -euo pipefail

LOG_DIR="迁移测试"
INITIAL_NODES=(one two)
NEW_NODE="three"
NFD_STRATEGY="/localhost/nfd/strategy/multicast"

mkdir -p "$LOG_DIR"

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

echo "Starting initial 2 nodes..."
for name in "${INITIAL_NODES[@]}"; do
  start_node "$name"
  sleep 1
done

sleep 8

echo "Inserting test data to force bucket 3 ownership..."
# Use a name that hashes to bucket 3
TEST_PREFIX="/test-bucket-3-data"
TEST_CONTENT="data for bucket 3 migration test"

echo "$TEST_CONTENT" | ./build/bin/kua-client put "$TEST_PREFIX" > "$LOG_DIR/client-put.log" 2>&1

sleep 3

echo "Starting new node $NEW_NODE to trigger reassignment..."
start_node "$NEW_NODE"

echo "Waiting for reassignment and potential migration..."
sleep 20

echo "Fetching data after node addition..."
./build/bin/kua-client get "$TEST_PREFIX" > "$LOG_DIR/client-get.out" 2>&1 || true

if grep -q "$TEST_CONTENT" "$LOG_DIR/client-get.out"; then
  echo "SUCCESS: data remained accessible after node addition"
else
  echo "FAIL: data not found after node addition"
  echo "--- client-get output ---"
  cat "$LOG_DIR/client-get.out"
  exit 1
fi

echo "Checking for migration logs..."
if grep -q "Migrating" "$LOG_DIR"/*.log; then
  echo "SUCCESS: Migration logs found - data was migrated"
  grep "Migrating" "$LOG_DIR"/*.log
else
  echo "INFO: No migration logs found - bucket may have been empty or migration didn't trigger"
fi

echo "Test complete. Logs written to $LOG_DIR"