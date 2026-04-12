#include "bidder.hpp"
#include "worker.hpp"

#include <ndn-cxx/util/logger.hpp>

#include <algorithm>
#include <set>
#include <string>
#include <vector>

namespace kua {

NDN_LOG_INIT(kua.bidder);

namespace {

uint64_t
computeHash(const std::string& value)
{
  return std::hash<std::string>{}(value);
}

} // namespace

Bidder::Bidder(ConfigBundle& configBundle, NodeWatcher& nodeWatcher)
  : m_configBundle(configBundle)
  , m_nodePrefix(configBundle.nodePrefix)
  , m_face(configBundle.face)
  , m_scheduler(m_face.getIoContext())
  , m_keyChain(configBundle.keyChain)
  , m_nodeWatcher(nodeWatcher)
{
  NDN_LOG_INFO("构造 Bidder");
  initialize();
}

void
Bidder::initialize()
{
  // Delay initial bucket assignment to allow nodes to discover each other
  m_scheduler.schedule(ndn::time::seconds(5), [this] { recomputeBucketAssignments(); });
}

void
Bidder::recomputeBucketAssignments()
{
  auto nodeList = m_nodeWatcher.getNodeList();
  if (nodeList.empty()) {
    NDN_LOG_DEBUG("未发现节点，跳过 bucket 分配");
    m_recomputeEvent = m_scheduler.schedule(ndn::time::seconds(3),
                                          [this] { recomputeBucketAssignments(); });
    return;
  }

  NDN_LOG_DEBUG("重新计算 bucket 分配，当前节点数: " << nodeList.size());
  for (bucket_id_t bucketId = 0; bucketId < NUM_BUCKETS; ++bucketId)
  {
    auto owners = computeBucketOwnersMaglev(nodeList, bucketId);
    std::set<ndn::Name> prevOwners(m_bucketOwners[bucketId].begin(), m_bucketOwners[bucketId].end());
    std::set<ndn::Name> newOwners(owners.begin(), owners.end());
    const bool wasLocalOwner = prevOwners.count(m_nodePrefix) > 0;
    const bool isLocalOwnerNow = newOwners.count(m_nodePrefix) > 0;

    if (wasLocalOwner && !isLocalOwnerNow)
    {
      auto bucketPtr = m_buckets[bucketId];
      if (bucketPtr && bucketPtr->worker)
      {
        bucketPtr->worker->migrateToOwners(owners);
        auto workerPtr = bucketPtr->worker;
        m_buckets.erase(bucketId);
        m_scheduler.schedule(ndn::time::seconds(10), [workerPtr] {
          workerPtr->stop();
        });
      }
      m_bucketOwners[bucketId] = owners;
      continue;
    }

    if (!wasLocalOwner && isLocalOwnerNow)
    {
      NDN_LOG_INFO("本地节点成为 bucket " << bucketId << " 的所有者");
      m_buckets[bucketId] = std::make_shared<Bucket>(bucketId);
      auto& bucket = *m_buckets[bucketId];
      bucket.confirmedHosts.clear();
      for (const auto& owner : owners)
        bucket.confirmedHosts[owner] = 1;
      bucket.worker = std::make_shared<Worker>(m_configBundle, bucket);
      m_bucketOwners[bucketId] = owners;
      continue;
    }

    if (isLocalOwnerNow)
    {
      if (!m_buckets.count(bucketId))
        m_buckets[bucketId] = std::make_shared<Bucket>(bucketId);

      auto& bucket = *m_buckets[bucketId];

      std::vector<ndn::Name> addedOwners;
      for (const auto& owner : owners)
      {
        if (!prevOwners.count(owner) && owner != m_nodePrefix)
          addedOwners.push_back(owner);
      }

      if (!addedOwners.empty() && bucket.worker)
        bucket.worker->migrateToOwners(addedOwners);

      bucket.confirmedHosts.clear();
      for (const auto& owner : owners)
        bucket.confirmedHosts[owner] = 1;

      if (!bucket.worker)
        bucket.worker = std::make_shared<Worker>(m_configBundle, bucket);

      m_bucketOwners[bucketId] = owners;
      continue;
    }

    m_bucketOwners[bucketId] = owners;
  }

  m_recomputeEvent = m_scheduler.schedule(ndn::time::seconds(3),
                                          [this] { recomputeBucketAssignments(); });
}

std::vector<ndn::Name>
Bidder::computeBucketOwnersMaglev(const std::vector<ndn::Name>& nodeList,
                                  bucket_id_t bucketId)
{
  std::vector<ndn::Name> owners;
  if (nodeList.empty())
    return owners;

  // Ensure every node derives the same table when the membership is identical.
  std::vector<ndn::Name> sortedNodes = nodeList;
  std::sort(sortedNodes.begin(), sortedNodes.end(), [] (const ndn::Name& a, const ndn::Name& b) {
    return a.toUri() < b.toUri();
  });

  // Maglev lookup table size. Prime helps reduce cycles in the permutation.
  static const size_t MAGLEV_TABLE_SIZE = 251;
  const size_t n = sortedNodes.size();

  std::vector<size_t> offset(n);
  std::vector<size_t> skip(n);
  std::vector<size_t> next(n, 0);
  std::vector<int> table(MAGLEV_TABLE_SIZE, -1);

  for (size_t i = 0; i < n; ++i)
  {
    const std::string key = sortedNodes[i].toUri();
    offset[i] = computeHash("maglev-offset:" + key) % MAGLEV_TABLE_SIZE;
    // skip in [1, M-1] so each backend permutation can cover full table.
    skip[i] = (computeHash("maglev-skip:" + key) % (MAGLEV_TABLE_SIZE - 1)) + 1;
  }

  // Fill Maglev table with backend indexes.
  size_t filled = 0;
  while (filled < MAGLEV_TABLE_SIZE)
  {
    for (size_t i = 0; i < n && filled < MAGLEV_TABLE_SIZE; ++i)
    {
      size_t c = (offset[i] + next[i] * skip[i]) % MAGLEV_TABLE_SIZE;
      while (table[c] >= 0)
      {
        ++next[i];
        c = (offset[i] + next[i] * skip[i]) % MAGLEV_TABLE_SIZE;
      }
      table[c] = static_cast<int>(i);
      ++next[i];
      ++filled;
    }
  }

  const size_t replicaCount = std::min<size_t>(NUM_REPLICA, n);
  const size_t start = computeHash("bucket:" + std::to_string(bucketId)) % MAGLEV_TABLE_SIZE;

  std::set<ndn::Name> selected;
  for (size_t step = 0; step < MAGLEV_TABLE_SIZE && selected.size() < replicaCount; ++step)
  {
    const size_t pos = (start + step) % MAGLEV_TABLE_SIZE;
    const int backendIdx = table[pos];
    if (backendIdx >= 0)
      selected.insert(sortedNodes[static_cast<size_t>(backendIdx)]);
  }

  owners.assign(selected.begin(), selected.end());
  return owners;
}

std::vector<ndn::Name>
Bidder::computeBucketOwnersClassic(const std::vector<ndn::Name>& nodeList,
                                   bucket_id_t bucketId)
{
  std::vector<ndn::Name> owners;
  if (nodeList.empty())
    return owners;

  static const unsigned int VIRTUAL_NODES = 8;
  struct RingEntry { uint64_t hash; ndn::Name node; };
  std::vector<RingEntry> ring;
  ring.reserve(nodeList.size() * VIRTUAL_NODES);

  for (const auto& node : nodeList)
  {
    for (unsigned int v = 0; v < VIRTUAL_NODES; ++v)
    {
      std::string key = node.toUri() + "#" + std::to_string(v);
      ring.push_back({ computeHash(key), node });
    }
  }

  std::sort(ring.begin(), ring.end(), [] (const RingEntry& a, const RingEntry& b) {
    return a.hash < b.hash;
  });

  uint64_t bucketHash = computeHash(std::to_string(bucketId));
  auto it = std::lower_bound(ring.begin(), ring.end(), bucketHash,
    [] (const RingEntry& entry, uint64_t value) {
      return entry.hash < value;
    });
  if (it == ring.end())
    it = ring.begin();

  std::set<ndn::Name> selected;
  while (selected.size() < std::min<size_t>(NUM_REPLICA, nodeList.size()))
  {
    selected.insert(it->node);
    ++it;
    if (it == ring.end())
      it = ring.begin();
  }

  owners.assign(selected.begin(), selected.end());
  return owners;
}

bool
Bidder::isLocalOwner(const std::vector<ndn::Name>& owners) const
{
  for (const auto& owner : owners)
    if (owner == m_nodePrefix)
      return true;
  return false;
}

} // namespace kua
