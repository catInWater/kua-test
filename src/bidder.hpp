#pragma once

#include <map>
#include <memory>
#include <vector>

#include "config-bundle.hpp"
#include "node-watcher.hpp"
#include "bucket.hpp"

namespace kua {

class Bidder
{
public:
  /** Initialize the bidder with the sync prefix */
  Bidder(ConfigBundle& configBundle, NodeWatcher& nodeWatcher);

private:
  void initialize();
  void recomputeBucketAssignments();
  // New default owner-selection method based on Maglev hashing.
  std::vector<ndn::Name> computeBucketOwnersMaglev(const std::vector<ndn::Name>& nodeList,
                                                   bucket_id_t bucketId);
  // Legacy ring-based consistent hashing kept for comparison/debugging.
  std::vector<ndn::Name> computeBucketOwnersClassic(const std::vector<ndn::Name>& nodeList,
                                                    bucket_id_t bucketId);
  bool isLocalOwner(const std::vector<ndn::Name>& owners) const;

private:
  ConfigBundle& m_configBundle;
  ndn::Name m_nodePrefix;
  ndn::Face& m_face;
  ndn::Scheduler m_scheduler;
  ndn::KeyChain& m_keyChain;
  NodeWatcher& m_nodeWatcher;

  std::map<bucket_id_t, std::shared_ptr<Bucket>> m_buckets;
  std::map<bucket_id_t, std::vector<ndn::Name>> m_bucketOwners;
  ndn::scheduler::ScopedEventId m_recomputeEvent;
};

} // namespace kua