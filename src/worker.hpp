#pragma once

#include <ndn-cxx/util/scheduler.hpp>

#include "config-bundle.hpp"
#include "bucket.hpp"
#include "store.hpp"
#include "nlsr.hpp"

namespace kua {

class Worker
{
public:
  Worker(ConfigBundle& configBundle, const Bucket& bucket);

  ~Worker();

private:
  void
  onRegisterFailed(const ndn::Name& prefix, const std::string& reason);

  void
  onInterest(const ndn::InterestFilter&, const ndn::Interest& interest);

  void
  run();

  void
  insert(const ndn::Name& dataName, const ndn::Interest& request, const uint64_t& commandCode);

  void
  insertNoReplicate(const ndn::Name& dataName, const ndn::Interest& request, const uint64_t& commandCode);

  void
  insertNoReplicateRange(const ndn::Name& dataName, const ndn::Interest& request, const uint64_t& commandCode);

  void
  replyInsert(const ndn::Interest& request);

    void
    replyText(const ndn::Interest& request, const std::string& text);

    void
    kvPut(const std::string& key, const std::string& value, uint64_t version,
        const ndn::Interest& request, const uint64_t& commandCode);

    void
    kvGet(const std::string& key, const ndn::Interest& request);

    void
    kvList(const ndn::Interest& request);

    static std::string
    decodeHex(const std::string& hex);

    static std::string
    encodeHex(const std::string& value);

  void
  fetch(const ndn::Interest& request);

public:
  void
  migrateToOwners(const std::vector<ndn::Name>& owners);

  void
  stop();

  std::shared_ptr<Store> store;

private:
  ConfigBundle& m_configBundle;
  const Bucket& m_bucket;

  ndn::Name m_nodePrefix;
  ndn::Face m_face;
  ndn::Scheduler m_scheduler;
  ndn::KeyChain& m_keyChain;

  ndn::Name m_bucketPrefix;
  ndn::Name m_bucketNodePrefix;

  std::shared_ptr<NLSR> nlsr;

  size_t m_failedRegistrations = 0;
};

} // namespace kua