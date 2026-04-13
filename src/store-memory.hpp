#pragma once

#include "store.hpp"
#include <map>
#include <string>
#include <vector>

namespace kua {

class StoreMemory : public Store
{
public:
  StoreMemory(bucket_id_t bucketId) : Store(bucketId) { }

  inline bool
  put(const ndn::Data& data)
  {
    std::shared_ptr<const ndn::Data> ptr = std::make_shared<const ndn::Data>(data);
    m_map[data.getName()] = ptr;
    return true;
  }

  inline std::shared_ptr<const ndn::Data>
  get(const ndn::Name& dataName)
  {
    if (m_map.find(dataName) != m_map.end())
    {
      return m_map.at(dataName);
    }
    else
    {
      return nullptr;
    }
  }

  inline std::vector<ndn::Name>
  getAllNames()
  {
    std::vector<ndn::Name> names;
    names.reserve(m_map.size());
    for (const auto& item : m_map)
      names.push_back(item.first);
    return names;
  }

  inline bool
  putKv(const std::string& key, const std::string& value, uint64_t version)
  {
    auto it = m_kvMap.find(key);
    if (it == m_kvMap.end() || version >= it->second.version)
    {
      m_kvMap[key] = KvItem{key, value, version};
      return true;
    }
    return false;
  }

  inline std::optional<KvItem>
  getKv(const std::string& key)
  {
    auto it = m_kvMap.find(key);
    if (it != m_kvMap.end())
      return it->second;
    return std::nullopt;
  }

  inline std::vector<KvItem>
  listKv()
  {
    std::vector<KvItem> items;
    items.reserve(m_kvMap.size());
    for (const auto& item : m_kvMap)
      items.push_back(item.second);
    return items;
  }

private:
  std::map<ndn::Name, std::shared_ptr<const ndn::Data>> m_map;
  std::map<std::string, KvItem> m_kvMap;
};

} // namespace kua