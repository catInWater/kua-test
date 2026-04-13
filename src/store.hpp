#pragma once

#include <ndn-cxx/data.hpp>
#include "bucket.hpp"
#include <optional>
#include <string>
#include <vector>

namespace kua {

struct KvItem
{
  std::string key;
  std::string value;
  uint64_t version;
};

class Store {
public:
  Store(bucket_id_t bucketId) {}

  virtual bool
  put(const ndn::Data& data) = 0;

  virtual std::shared_ptr<const ndn::Data>
  get(const ndn::Name& dataName) = 0;

  virtual std::vector<ndn::Name>
  getAllNames() = 0;

  virtual bool
  putKv(const std::string& key, const std::string& value, uint64_t version) = 0;

  virtual std::optional<KvItem>
  getKv(const std::string& key) = 0;

  virtual std::vector<KvItem>
  listKv() = 0;

  virtual ~Store() = default;
};

} // namespace kua