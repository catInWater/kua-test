#include "bidder.hpp"
#include "worker.hpp"

#include <ndn-cxx/util/logger.hpp>

#include <algorithm>
#include <set>
#include <string>

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
  NDN_LOG_INFO("Constructing Bidder");
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
    NDN_LOG_DEBUG("No nodes discovered yet, skipping bucket assignment");
    m_recomputeEvent = m_scheduler.schedule(ndn::time::seconds(3),
                                          [this] { recomputeBucketAssignments(); });
    return;
  }

  NDN_LOG_DEBUG("Recomputing bucket assignments with " << nodeList.size() << " nodes");
  for (bucket_id_t bucketId = 0; bucketId < NUM_BUCKETS; ++bucketId)
  {
    auto owners = computeBucketOwners(nodeList, bucketId);
    if (!isLocalOwner(owners))
      continue;

    NDN_LOG_INFO("Local node is owner of bucket " << bucketId);
    if (!m_buckets.count(bucketId))
      m_buckets[bucketId] = std::make_shared<Bucket>(bucketId);

    auto& bucket = *m_buckets[bucketId];
    bucket.confirmedHosts.clear();
    for (const auto& owner : owners)
      bucket.confirmedHosts[owner] = 1;

    if (!bucket.worker)
      bucket.worker = std::make_shared<Worker>(m_configBundle, bucket);
  }

  m_recomputeEvent = m_scheduler.schedule(ndn::time::seconds(3),
                                          [this] { recomputeBucketAssignments(); });
}

std::vector<ndn::Name>
Bidder::computeBucketOwners(const std::vector<ndn::Name>& nodeList,
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
