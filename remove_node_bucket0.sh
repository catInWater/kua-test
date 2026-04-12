#!/bin/bash

set -euo pipefail

LOG_DIR="删除节点_bucket0"
NODES=(one two three four five)
NFD_STRATEGY="/localhost/nfd/strategy/multicast"
NUM_BUCKETS=4
TARGET_BUCKET=0
TARGET_ITEMS=6

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

# Build a tiny helper that computes bucket id using the same hash rule as Bucket::idFromName.
HELPER_SRC="/tmp/kua_find_bucket_prefix.cpp"
HELPER_BIN="/tmp/kua_find_bucket_prefix"
cat > "$HELPER_SRC" <<'EOF'
#include <ndn-cxx/name.hpp>
#include <iostream>
#include <functional>
#include <string>

int main(int argc, char** argv)
{
  if (argc != 4) {
    std::cerr << "usage: <targetBucket> <numBuckets> <count>\n";
    return 1;
  }

  const int target = std::stoi(argv[1]);
  const int numBuckets = std::stoi(argv[2]);
  const int count = std::stoi(argv[3]);

  std::hash<ndn::Name> hashFunc;
  int found = 0;
  for (int i = 0; i < 200000 && found < count; ++i) {
    std::string s = "/bucket0-test-" + std::to_string(i);
    ndn::Name name(s);
    auto id = static_cast<int>(hashFunc(name) % static_cast<size_t>(numBuckets));
    if (id == target) {
      std::cout << s << "\n";
      ++found;
    }
  }

  return found == count ? 0 : 2;
}
EOF

g++ -std=c++17 "$HELPER_SRC" -o "$HELPER_BIN" $(pkg-config --cflags --libs libndn-cxx) >/dev/null 2>&1

echo "Generating prefixes that map to bucket #$TARGET_BUCKET..."
mapfile -t BUCKET0_PREFIXES < <("$HELPER_BIN" "$TARGET_BUCKET" "$NUM_BUCKETS" "$TARGET_ITEMS")

if [ "${#BUCKET0_PREFIXES[@]}" -lt "$TARGET_ITEMS" ]; then
  echo "FAIL: unable to find enough bucket #$TARGET_BUCKET prefixes"
  exit 1
fi

printf '%s\n' "${BUCKET0_PREFIXES[@]}" > "$LOG_DIR/bucket0-prefixes.txt"
echo "Bucket #$TARGET_BUCKET prefixes:"
cat "$LOG_DIR/bucket0-prefixes.txt"

echo "Detecting current owners of bucket #$TARGET_BUCKET..."
BUCKET0_OWNERS=()
for node in "${NODES[@]}"; do
  if grep -q "本地节点成为 bucket $TARGET_BUCKET 的所有者" "$LOG_DIR/$node.log"; then
    BUCKET0_OWNERS+=("$node")
  fi
done

if [ "${#BUCKET0_OWNERS[@]}" -eq 0 ]; then
  echo "FAIL: could not detect any owner for bucket #$TARGET_BUCKET"
  exit 1
fi

REMOVE_NODE="${BUCKET0_OWNERS[0]}"
echo "Bucket #$TARGET_BUCKET owners before removal: ${BUCKET0_OWNERS[*]}"
echo "Will remove owner node: $REMOVE_NODE"

echo "Inserting bucket #$TARGET_BUCKET data before node removal..."
for prefix in "${BUCKET0_PREFIXES[@]}"; do
  content="prefail bucket0 data for $prefix"
  echo "$content" | ./build/bin/kua-client put "$prefix" >> "$LOG_DIR/client-put.log" 2>&1
  sleep 1
done

sleep 6

echo "Stopping owner node $REMOVE_NODE"
sudo pkill -f "./build/bin/kua /kua /$REMOVE_NODE" || true
sleep 1

echo "Waiting for reassignment and migration..."
sleep 28

echo "Verifying all bucket #$TARGET_BUCKET data remain accessible..."
SUCCESS=0
TOTAL=${#BUCKET0_PREFIXES[@]}

for prefix in "${BUCKET0_PREFIXES[@]}"; do
  expected="prefail bucket0 data for $prefix"
  outFile="$LOG_DIR/client-get-${prefix##*/}.out"
  ./build/bin/kua-client get "$prefix" > "$outFile" 2>&1 || true
  if grep -q "$expected" "$outFile"; then
    echo "SUCCESS: $prefix accessible"
    SUCCESS=$((SUCCESS + 1))
  else
    echo "FAIL: $prefix missing"
    cat "$outFile"
  fi
done

if [ "$SUCCESS" -ne "$TOTAL" ]; then
  echo "FAIL: only $SUCCESS/$TOTAL bucket #$TARGET_BUCKET items accessible"
  exit 1
fi

echo "SUCCESS: all $TOTAL bucket #$TARGET_BUCKET items accessible"

echo "Checking bucket #$TARGET_BUCKET migration logs..."
if grep -qE "bucket #0 .*迁移|发送迁移请求.*bucket0-test|接收数据并存储成功: /bucket0-test-.* 到 bucket #0" "$LOG_DIR"/*.log; then
  grep -E "bucket #0 .*迁移|发送迁移请求.*bucket0-test|接收数据并存储成功: /bucket0-test-.* 到 bucket #0" "$LOG_DIR"/*.log > "$LOG_DIR/bucket0-migration-summary.log" || true
  echo "SUCCESS: found bucket #$TARGET_BUCKET migration evidence"
  echo "Summary: $LOG_DIR/bucket0-migration-summary.log"
else
  echo "WARN: no explicit bucket #$TARGET_BUCKET migration log found"
fi

echo "Test complete. Logs written to $LOG_DIR"
