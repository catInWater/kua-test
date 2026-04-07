#include "worker.hpp"
#include "store-memory.hpp"
#include "command-codes.hpp"

#include <ndn-cxx/util/logger.hpp>
#include <thread>
#include <vector>

namespace kua {

NDN_LOG_INIT(kua.worker);

Worker::Worker(ConfigBundle& configBundle, const Bucket& bucket)
  : m_configBundle(configBundle)
  , m_bucket(bucket)
  , m_nodePrefix(configBundle.nodePrefix)
  , m_scheduler(m_face.getIoContext())
  , m_keyChain(configBundle.keyChain)
  , m_bucketPrefix(ndn::Name(configBundle.kuaPrefix).appendNumber(bucket.id))
  , m_bucketNodePrefix(ndn::Name(m_nodePrefix).appendNumber(bucket.id))
{
  NDN_LOG_INFO("构造 worker #" << bucket.id << " " << m_nodePrefix);

  // Make NLSR controller
  nlsr = std::make_shared<NLSR>(m_keyChain, m_face);

  // Make data store
  this->store = std::make_shared<StoreMemory>(bucket.id);

  // Get all interests
  m_face.setInterestFilter("/", std::bind(&Worker::onInterest, this, _1, _2));

  // Register for unique node

  m_face.registerPrefix(m_bucketNodePrefix,
                        [this] (const auto&) {
                          nlsr->advertise(m_bucketNodePrefix);
                        },
                        std::bind(&Worker::onRegisterFailed, this, _1, _2));

  // Register for bucket
  m_face.registerPrefix(m_bucketPrefix,
                        [this] (const auto&) {
                          nlsr->advertise(m_bucketPrefix);
                        },
                        std::bind(&Worker::onRegisterFailed, this, _1, _2));

  std::thread thread(std::bind(&Worker::run, this));
  thread.detach();
}

Worker::~Worker() {
  m_face.shutdown();
}

void
Worker::run()
{
  m_face.processEvents();
}

void
Worker::onRegisterFailed(const ndn::Name& prefix, const std::string& reason)
{
  NDN_LOG_ERROR("错误：注册前缀 '" << prefix
             << "' 到本地转发器失败（" << reason << "）");

  if (m_failedRegistrations >= 50) {
    NDN_LOG_ERROR("严重错误：注册前缀 '" << prefix
             << "' 到本地转发器失败次数过多（" << reason << "）");
    m_face.shutdown();
    return;
  }

  m_scheduler.schedule(ndn::time::milliseconds(300), [this, prefix] {
    m_face.registerPrefix(prefix,
                          nullptr, // RegisterPrefixSuccessCallback is optional
                          std::bind(&Worker::onRegisterFailed, this, _1, _2));
  });

  m_failedRegistrations += 1;
}

void
Worker::onInterest(const ndn::InterestFilter&, const ndn::Interest& interest)
{
  auto reqName = interest.isSigned() ? interest.getName().getPrefix(-1) : interest.getName();

  // Ignore interests from localhost
  if (ndn::Name("localhost").isPrefixOf(reqName)) return;

  NDN_LOG_DEBUG("收到请求 : #" << m_bucket.id << " : " << reqName);

  // Command Code
  if (reqName.size() > 1 && reqName[-1].isNumber() &&
      (m_bucketPrefix.isPrefixOf(reqName) || m_nodePrefix.isPrefixOf(reqName)))
  {
    uint64_t ccode = reqName[-1].toNumber();

    if (ccode & CommandCodes::INSERT)
    {
      ndn::Name insertName(reqName.get(-2).blockFromValue());
      insert(insertName, interest, ccode);
      return;
    }
  }

  // FETCH command
  for (const auto& delegation : interest.getForwardingHint())
    if (delegation.size() > 1 && delegation[-1].isNumber() &&
        delegation[-1].toNumber() == CommandCodes::FETCH)
      return this->fetch(interest);
}

void
Worker::insert(const ndn::Name& dataName, const ndn::Interest& request, const uint64_t& commandCode)
{
  if (commandCode & CommandCodes::NO_REPLICATE)
    return insertNoReplicate(dataName, request, commandCode);

  std::shared_ptr<int> replicaCount = std::make_shared<int>(0);

  for (const auto& host : m_bucket.confirmedHosts)
  {
    // Interest
    ndn::Name interestName(host.first);
    interestName.appendNumber(m_bucket.id);
    interestName.append(dataName.wireEncode());
    interestName.appendNumber(commandCode | CommandCodes::NO_REPLICATE);

    ndn::Interest interest(interestName);
    interest.setCanBePrefix(false);
    interest.setMustBeFresh(true);

    // Signature
    ndn::security::SigningInfo interestSigningInfo;
    interestSigningInfo.setSha256Signing();
    interestSigningInfo.setSignedInterestFormat(ndn::security::SignedInterestFormat::V03);
    m_keyChain.sign(interest, interestSigningInfo);


    // Replicate at all replicas
    m_face.expressInterest(interest, [this, request, replicaCount, dataName] (const auto&, const auto& data) {
      (*replicaCount)++;
      NDN_LOG_TRACE("#" << m_bucket.id << " : 插入成功副本 : "
                  << data.getName() << " : 副本 " << *replicaCount);

      // 所有副本完成
      if ((*replicaCount) >= NUM_REPLICA)
      {
        NDN_LOG_DEBUG("#" << m_bucket.id << " : 所有副本已完成 : " << dataName);
        replyInsert(request);
      }
    }, nullptr, nullptr);
  }
}

void
Worker::insertNoReplicate(const ndn::Name& dataName, const ndn::Interest& request,
                          const uint64_t& commandCode)
{
  if (commandCode & CommandCodes::IS_RANGE)
    return insertNoReplicateRange(dataName, request, commandCode);

  bool isMigration = (commandCode & CommandCodes::MIGRATE) != 0;

  if (isMigration) {
    NDN_LOG_INFO("接收数据: " << dataName << " 到 bucket #" << m_bucket.id);
  }

  // 请求原始数据
  ndn::Interest interest(dataName);

  interest.setCanBePrefix(false);
  interest.setMustBeFresh(false);
  interest.setInterestLifetime(request.getInterestLifetime());

  m_face.expressInterest(interest, [this, request, dataName, isMigration] (const auto&, const auto& data) {
    if (store->put(data)) {
      if (isMigration) {
        NDN_LOG_INFO("接收数据并存储成功: " << dataName << " 到 bucket #" << m_bucket.id);
      }
      replyInsert(request);
    } else {
      if (isMigration) {
        NDN_LOG_ERROR("接收数据存储失败: " << dataName << " 到 bucket #" << m_bucket.id << " 失败");
      }
      NDN_LOG_TRACE("#" << m_bucket.id << " : 存储失败 : " << data.getName());
    }
  }, nullptr, nullptr);
}

void
Worker::insertNoReplicateRange(const ndn::Name& dataName, const ndn::Interest& request,
                               const uint64_t& commandCode)
{
  if (dataName.size() <= 2 || !dataName[-1].isSegment() || !dataName[-2].isSegment())
    return;

  const auto startSeg = dataName[-2].toSegment();
  const auto endSeg = dataName[-1].toSegment();

  ndn::Name dataNamePrefix(dataName.getPrefix(-2));

  const auto fetchedCount = std::make_shared<uint64_t>(0);

  for (auto currSeg = startSeg; currSeg <= endSeg; currSeg++)
  {
    // Request data
    ndn::Name interestName(dataNamePrefix);
    interestName.appendSegment(currSeg);

    ndn::Interest interest(interestName);
    interest.setCanBePrefix(false);
    interest.setMustBeFresh(false);
    interest.setInterestLifetime(request.getInterestLifetime());

    m_face.expressInterest(interest,
      [this, fetchedCount, endSeg, startSeg, request] (const auto&, const auto& data)
    {
      NDN_LOG_DEBUG("FETCH " << *fetchedCount << " / " << endSeg - startSeg + 1);

      if (store->put(data))
        (*fetchedCount)++;
      else
        NDN_LOG_TRACE("#" << m_bucket.id << " : 存储失败 : " << data.getName());

      if (*fetchedCount == endSeg - startSeg + 1)
        replyInsert(request);
    }, nullptr, nullptr);
  }
}

void
Worker::replyInsert(const ndn::Interest& request)
{
  NDN_LOG_TRACE("#" << m_bucket.id << " : 插入成功回复 : " << request);
  ndn::Data response(request.getName());
  response.setFreshnessPeriod(ndn::time::seconds(10));
  ndn::security::SigningInfo info;
  info.setSha256Signing();
  m_keyChain.sign(response, info);
  m_face.put(response);
}

void
Worker::migrateToOwners(const std::vector<ndn::Name>& owners)
{
  auto names = store->getAllNames();
  if (names.empty() || owners.empty())
  {
    NDN_LOG_INFO("bucket #" << m_bucket.id << " 无需迁移数据（names: " << names.size() 
                << ", owners: " << owners.size() << "）");
    return;
  }

  const auto migrationDelay = ndn::time::seconds(5);
  NDN_LOG_INFO("等待 " << migrationDelay.count() << " 秒后开始迁移 bucket #" << m_bucket.id << " 的 " << names.size() << " 条数据到 " 
               << owners.size() << " 个新所有者");

  m_scheduler.schedule(migrationDelay, [this, owners, names] {
    NDN_LOG_INFO("开始迁移 bucket #" << m_bucket.id << " 的 " << names.size() << " 条数据到 " 
                 << owners.size() << " 个新所有者");

    size_t totalMigrations = 0;
    for (const auto& owner : owners)
    {
      if (owner == m_nodePrefix)
      {
        NDN_LOG_DEBUG("跳过自己作为所有者: " << owner);
        continue;
      }

      NDN_LOG_INFO("迁移 bucket #" << m_bucket.id << " 到所有者: " << owner << " (" << names.size() << " 条数据)");
      
      for (const auto& dataName : names)
      {
        ndn::Name interestName(owner);
        interestName.appendNumber(m_bucket.id);
        interestName.append(dataName.wireEncode());
        interestName.appendNumber(CommandCodes::INSERT | CommandCodes::NO_REPLICATE | CommandCodes::MIGRATE);

        ndn::Interest interest(interestName);
        interest.setCanBePrefix(false);
        interest.setMustBeFresh(true);

        ndn::security::SigningInfo interestSigningInfo;
        interestSigningInfo.setSha256Signing();
        interestSigningInfo.setSignedInterestFormat(ndn::security::SignedInterestFormat::V03);
        m_keyChain.sign(interest, interestSigningInfo);

        totalMigrations++;
        NDN_LOG_DEBUG("发送迁移请求 #" << totalMigrations << ": " << dataName << " -> " << owner);

        m_face.expressInterest(interest,
          [this, dataName, owner, totalMigrations] (const auto&, const auto& data) {
            NDN_LOG_INFO("迁移成功 #" << totalMigrations << ": " << dataName << " 已发送到 " << owner);
          },
          [this, dataName, owner, totalMigrations] (const auto&, const auto&) {
            NDN_LOG_ERROR("迁移失败 #" << totalMigrations << ": " << dataName << " 发送到 " << owner << " 失败");
          },
          nullptr);
      }
    }
    
    NDN_LOG_INFO("bucket #" << m_bucket.id << " 迁移完成，共发送 " << totalMigrations << " 条数据");
  });
}

void
Worker::stop()
{
  auto names = store->getAllNames();
  NDN_LOG_INFO("停止 worker #" << m_bucket.id << " (" << m_nodePrefix << ")，包含 " << names.size() << " 条数据");
  if (!names.empty()) {
    NDN_LOG_INFO("worker #" << m_bucket.id << " 停止前数据列表:");
    for (const auto& name : names) {
      NDN_LOG_INFO("  - " << name);
    }
  }
  m_face.shutdown();
}

void
Worker::fetch(const ndn::Interest& request)
{
  auto data = this->store->get(request.getName());
  if (data)
    m_face.put(*data);
}

} // namespace kua