#!/bin/bash

set -euo pipefail

LOG_DIR="KV阶段1"
NODES=(one two three four)
NFD_STRATEGY="/localhost/nfd/strategy/multicast"
TIMES_FILE="$LOG_DIR/figure-times.tsv"
EVENTS_FILE="$LOG_DIR/kv-events.tsv"

record_marker() {
  local label="$1"
  printf '%s\t%s\n' "$label" "$(date +%s.%N)" >> "$TIMES_FILE"
}

record_kv_event() {
  local label="$1"
  local key="$2"
  local value="$3"
  printf '%s\t%s\t%s\t%s\n' "$(date +%s.%N)" "$label" "$key" "$value" >> "$EVENTS_FILE"
}

if [ -d "$LOG_DIR" ]; then
  echo "Cleaning old log files in $LOG_DIR"
  rm -rf "$LOG_DIR"/* "$LOG_DIR"/.[!.]* "$LOG_DIR"/..?* 2>/dev/null || true
else
  mkdir -p "$LOG_DIR"
fi

: > "$TIMES_FILE"
: > "$EVENTS_FILE"

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
record_kv_event "userA-v1" "userA" "v1"
sleep 1
./build/bin/kua-client kv-get userA > "$LOG_DIR/kv-get-userA-v1.out" 2> "$LOG_DIR/kv-get-userA-v1.err"
if ! grep -qx "v1" "$LOG_DIR/kv-get-userA-v1.out"; then
  echo "FAIL: expected userA=v1"
  cat "$LOG_DIR/kv-get-userA-v1.out"
  exit 1
fi
record_marker "首次写入后"

echo "Test 2: KV update should return latest value"
./build/bin/kua-client kv-put userA v2 > "$LOG_DIR/kv-put-userA-v2.out" 2> "$LOG_DIR/kv-put-userA-v2.err"
record_kv_event "userA-v2" "userA" "v2"
sleep 1
./build/bin/kua-client kv-get userA > "$LOG_DIR/kv-get-userA-v2.out" 2> "$LOG_DIR/kv-get-userA-v2.err"
if ! grep -qx "v2" "$LOG_DIR/kv-get-userA-v2.out"; then
  echo "FAIL: expected userA=v2 after update"
  cat "$LOG_DIR/kv-get-userA-v2.out"
  exit 1
fi
record_marker "更新写入后"

echo "Test 3: Add another key and list all"
./build/bin/kua-client kv-put userB b1 > "$LOG_DIR/kv-put-userB-b1.out" 2> "$LOG_DIR/kv-put-userB-b1.err"
record_kv_event "userB-b1" "userB" "b1"
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

record_marker "列表验证后"

python3 draw_kv_distribution_snapshots.py \
  --log-dir "$LOG_DIR" \
  --times-file "$TIMES_FILE" \
  --events-file "$EVENTS_FILE" \
  --output "$LOG_DIR/kv-phase1-distribution.svg" \
  --summary-output "$LOG_DIR/kv-phase1-distribution.txt" \
  --title "KV阶段1：基本读写与列表的数据分布"

echo "SUCCESS: phase-1 KV tests passed"
echo "Logs in $LOG_DIR"
