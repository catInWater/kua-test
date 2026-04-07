#!/bin/bash

set -euo pipefail

LOG_DIR="log2"
NODES=(one two three four)
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

for name in "${NODES[@]}"; do
  start_node "$name"
  sleep 1
done

sleep 5

echo "All nodes started. Waiting for consistent-hash bucket assignment..."
sleep 5

TEST_PREFIX="/test"
TEST_CONTENT="hello kua consistent hash"

# Put data
echo "Putting data to $TEST_PREFIX"
echo "$TEST_CONTENT" | ./build/bin/kua-client put "$TEST_PREFIX" > "$LOG_DIR/client-put.log" 2>&1

# Give the system a moment to process and store chunks
sleep 3

echo "Getting data from $TEST_PREFIX"
./build/bin/kua-client get "$TEST_PREFIX" > "$LOG_DIR/client-get.out" 2>&1 || true

if grep -q "$TEST_CONTENT" "$LOG_DIR/client-get.out"; then
  echo "SUCCESS: data fetched correctly"
else
  echo "FAIL: data not found in client-get output"
  echo "--- client-get output ---"
  cat "$LOG_DIR/client-get.out"
fi

echo "Logs written to $LOG_DIR"