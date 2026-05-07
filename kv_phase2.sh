#!/bin/bash

set -euo pipefail

LOG_DIR="KV阶段2"
NODES=(one two three four)
NFD_STRATEGY="/localhost/nfd/strategy/multicast"
KEY="userQ"
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

echo "Get key bucket id"
cat <<'EOF' | g++ -std=c++17 -x c++ - -o /tmp/kua_kv_bucket $(pkg-config --cflags --libs libndn-cxx) >/dev/null 2>&1
#include <ndn-cxx/name.hpp>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <functional>
int main(int argc, char** argv) {
  if (argc != 2) return 1;
  std::string key = argv[1];
  std::ostringstream hex;
  hex << std::hex << std::setfill('0');
  for (unsigned char c : key) hex << std::setw(2) << static_cast<int>(c);
  ndn::Name n("/kv");
  n.append(hex.str());
  std::hash<ndn::Name> h;
  std::cout << (h(n) % 4);
  return 0;
}
EOF

BUCKET_ID=$(/tmp/kua_kv_bucket "$KEY")

echo "Bucket for $KEY is #$BUCKET_ID"

echo "Detect bucket owners from logs"
OWNERS=()
for n in "${NODES[@]}"; do
  if grep -q "本地节点成为 bucket $BUCKET_ID 的所有者" "$LOG_DIR/$n.log"; then
    OWNERS+=("$n")
  fi
done

if [ "${#OWNERS[@]}" -lt 3 ]; then
  echo "FAIL: expected >=3 owners for bucket #$BUCKET_ID, got ${#OWNERS[@]}"
  printf '%s\n' "${OWNERS[@]}"
  exit 1
fi

echo "Owners: ${OWNERS[*]}"
DOWN_NODE="${OWNERS[0]}"

echo "Step1: initial write"
./build/bin/kua-client kv-put "$KEY" v1 > "$LOG_DIR/kv-put-v1.out" 2> "$LOG_DIR/kv-put-v1.err"
record_kv_event "initial-v1" "$KEY" "v1"
sleep 1

./build/bin/kua-client kv-get "$KEY" > "$LOG_DIR/kv-get-v1.out" 2> "$LOG_DIR/kv-get-v1.err"
if ! grep -qx "v1" "$LOG_DIR/kv-get-v1.out"; then
  echo "FAIL: expected v1"
  cat "$LOG_DIR/kv-get-v1.out"
  exit 1
fi
record_marker "初始写入后"

echo "Step2: take down one owner $DOWN_NODE"
sudo pkill -f "./build/bin/kua /kua /$DOWN_NODE" || true
sleep 5

echo "Step3: write new value with one owner down (W=2)"
./build/bin/kua-client kv-put "$KEY" v2 > "$LOG_DIR/kv-put-v2.out" 2> "$LOG_DIR/kv-put-v2.err"
record_kv_event "quorum-v2" "$KEY" "v2"
if ! grep -q "OK" "$LOG_DIR/kv-put-v2.err"; then
  echo "FAIL: kv-put v2 did not satisfy write quorum"
  cat "$LOG_DIR/kv-put-v2.err"
  exit 1
fi
record_marker "单副本故障写入后"
LATEST_VERSION=$(grep -h "KV 写入成功: key=$KEY" "$LOG_DIR"/*.log | sed -E 's/.*version=([0-9]+).*/\1/' | sort -n | tail -n1)

echo "Step4: restart downed owner"
start_node "$DOWN_NODE"
sleep 12

echo "Step5: read latest value with quorum + read-repair"
./build/bin/kua-client kv-get "$KEY" > "$LOG_DIR/kv-get-v2.out" 2> "$LOG_DIR/kv-get-v2.err"
if ! grep -qx "v2" "$LOG_DIR/kv-get-v2.out"; then
  echo "FAIL: expected latest v2"
  cat "$LOG_DIR/kv-get-v2.out"
  exit 1
fi

echo "Step5b: repeat read until repaired owner catches latest version"
for attempt in 1 2 3 4 5; do
  if grep -q "KV 写入成功: key=$KEY, version=$LATEST_VERSION" "$LOG_DIR/$DOWN_NODE.log"; then
    break
  fi
  ./build/bin/kua-client kv-get "$KEY" > "$LOG_DIR/kv-get-repair-$attempt.out" 2> "$LOG_DIR/kv-get-repair-$attempt.err"
  sleep 2
done

if ! grep -q "KV 写入成功: key=$KEY, version=$LATEST_VERSION" "$LOG_DIR/$DOWN_NODE.log"; then
  echo "FAIL: repaired owner $DOWN_NODE did not receive latest version $LATEST_VERSION"
  exit 1
fi

record_marker "读修复后"

echo "Step6: list keys should show key with latest version"
./build/bin/kua-client kv-list "$BUCKET_ID" > "$LOG_DIR/kv-list-bucket.out" 2> "$LOG_DIR/kv-list-bucket.err"
if ! grep -q "^$KEY[[:space:]]" "$LOG_DIR/kv-list-bucket.out"; then
  echo "FAIL: kv-list missing $KEY"
  cat "$LOG_DIR/kv-list-bucket.out"
  exit 1
fi

echo "Step7: verify repaired owner received v2"
if grep -q "KV 写入成功: key=$KEY, version=$LATEST_VERSION" "$LOG_DIR/$DOWN_NODE.log"; then
  echo "INFO: repaired owner has KV write logs"
else
  echo "WARN: no explicit repair log found on $DOWN_NODE"
fi

python3 draw_kv_distribution_snapshots.py \
  --log-dir "$LOG_DIR" \
  --times-file "$TIMES_FILE" \
  --events-file "$EVENTS_FILE" \
  --output "$LOG_DIR/kv-phase2-distribution.svg" \
  --summary-output "$LOG_DIR/kv-phase2-distribution.txt" \
  --title "KV阶段2：法定人数写入与读修复的数据分布"

echo "SUCCESS: phase-2 quorum/read-repair test passed"
echo "Logs in $LOG_DIR"
