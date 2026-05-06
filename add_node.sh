#!/bin/bash

set -euo pipefail

LOG_DIR="添加节点"
INITIAL_NODES=(one two three four)
NEW_NODE="five"
NFD_STRATEGY="/localhost/nfd/strategy/multicast"
TIMES_FILE="$LOG_DIR/figure-times.tsv"

record_marker() {
  local label="$1"
  printf '%s\t%s\n' "$label" "$(date +%s.%N)" >> "$TIMES_FILE"
}

if [ -d "$LOG_DIR" ]; then
  echo "Cleaning old log files in $LOG_DIR"
  rm -rf "$LOG_DIR"/* "$LOG_DIR"/.[!.]* "$LOG_DIR"/..?* 2>/dev/null || true
else
  mkdir -p "$LOG_DIR"
fi

: > "$TIMES_FILE"

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

echo "Starting initial 4 nodes..."
for name in "${INITIAL_NODES[@]}"; do
  start_node "$name"
  sleep 1
done

sleep 10
record_marker "扩容前"

echo "Initial nodes started. Inserting pre-join data into bucket 1 and bucket 3..."
PRE_JOIN_DATA=(
  "/gamma2:prejoin bucket1 item1"
  "/delta4:prejoin bucket1 item2"
  "/gamma7:prejoin bucket1 item3"
  "/alpha2:prejoin bucket3 item1"
  "/beta2:prejoin bucket3 item2"
  "/alpha7:prejoin bucket3 item3"
)

for data in "${PRE_JOIN_DATA[@]}"; do
  IFS=':' read -r prefix content <<< "$data"
  echo "Inserting pre-join data to $prefix"
  echo "$content" | ./build/bin/kua-client put "$prefix" >> "$LOG_DIR/client-put.log" 2>&1
  sleep 1
done

sleep 5

echo "Starting new node $NEW_NODE..."
start_node "$NEW_NODE"

echo "Waiting for node $NEW_NODE to stabilize before migration..."
sleep 12
record_marker "扩容检测后"
sleep 28
record_marker "迁移完成后"

echo "Inserting post-join data into bucket 1 and bucket 3..."
POST_JOIN_DATA=(
  "/delta6:postjoin bucket1 item1"
  "/alpha8:postjoin bucket1 item2"
  "/beta7:postjoin bucket3 item1"
  "/gamma8:postjoin bucket3 item2"
)

for data in "${POST_JOIN_DATA[@]}"; do
  IFS=':' read -r prefix content <<< "$data"
  echo "Inserting post-join data to $prefix"
  echo "$content" | ./build/bin/kua-client put "$prefix" >> "$LOG_DIR/client-put.log" 2>&1
  sleep 1
done

sleep 5

echo "Fetching all data after node addition..."
SUCCESS_COUNT=0
TOTAL_TESTS=$(( ${#PRE_JOIN_DATA[@]} + ${#POST_JOIN_DATA[@]} ))

for data in "${PRE_JOIN_DATA[@]}" "${POST_JOIN_DATA[@]}"; do
  IFS=':' read -r prefix content <<< "$data"
  echo "Getting data from $prefix"
  ./build/bin/kua-client get "$prefix" > "$LOG_DIR/client-get-${prefix##*/}.out" 2>&1 || true

  if grep -q "$content" "$LOG_DIR/client-get-${prefix##*/}.out"; then
    echo "SUCCESS: $prefix data accessible"
    SUCCESS_COUNT=$((SUCCESS_COUNT + 1))
  else
    echo "FAIL: $prefix data not found"
    echo "--- $prefix output ---"
    cat "$LOG_DIR/client-get-${prefix##*/}.out"
  fi
done

if [ "$SUCCESS_COUNT" -eq "$TOTAL_TESTS" ]; then
  echo "SUCCESS: All $TOTAL_TESTS data items remained accessible after node addition"
else
  echo "FAIL: Only $SUCCESS_COUNT/$TOTAL_TESTS data items accessible after node addition"
  exit 1
fi

echo "Checking whether node $NEW_NODE received migrated data for bucket 1 and bucket 3..."
if grep -qE "接收数据: /gamma2/seg=0|接收数据: /delta4/seg=0|接收数据: /gamma7/seg=0|接收数据: /alpha2/seg=0|接收数据: /beta2/seg=0|接收数据: /alpha7/seg=0" "$LOG_DIR/$NEW_NODE.log"; then
  echo "SUCCESS: Node $NEW_NODE received migrated data in bucket 1 and bucket 3"
else
  echo "FAIL: Node $NEW_NODE did not receive expected migrated data"
  echo "--- $NEW_NODE migration log ---"
  grep -E "接收数据: /gamma2|接收数据: /delta4|接收数据: /gamma7|接收数据: /alpha2|接收数据: /beta2|接收数据: /alpha7" "$LOG_DIR/$NEW_NODE.log" || true
  exit 1
fi

echo "Checking whether node $NEW_NODE handled new writes after join..."
if grep -qE "收到请求 : #1 .*delta6|收到请求 : #1 .*alpha8|收到请求 : #3 .*beta7|收到请求 : #3 .*gamma8" "$LOG_DIR/$NEW_NODE.log"; then
  echo "SUCCESS: Node $NEW_NODE handled post-join writes for bucket 1 and bucket 3"
else
  echo "INFO: Node $NEW_NODE may not show the exact post-join request lines in logs"
  echo "--- $NEW_NODE insert log ---"
  grep -E "收到请求 : #1|收到请求 : #3" "$LOG_DIR/$NEW_NODE.log" || true
fi

python3 draw_bucket_assignment_snapshots.py \
  --log-dir "$LOG_DIR" \
  --times-file "$TIMES_FILE" \
  --output "$LOG_DIR/hot-expand-bucket-snapshots.svg" \
  --summary-output "$LOG_DIR/hot-expand-bucket-snapshots.txt" \
  --title "热扩容实验中的桶分配变化"

echo "Test complete. Logs written to $LOG_DIR"
