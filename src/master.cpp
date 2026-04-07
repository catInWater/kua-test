#include "master.hpp"

#include <ndn-cxx/util/logger.hpp>

namespace kua {

NDN_LOG_INIT(kua.master);

Master::Master(ConfigBundle& configBundle, NodeWatcher& nodeWatcher)
  : m_configBundle(configBundle)
  , m_syncPrefix(ndn::Name(configBundle.kuaPrefix).append("sync").append("auction"))
  , m_nodePrefix(configBundle.nodePrefix)
  , m_face(configBundle.face)
  , m_scheduler(m_face.getIoContext())
  , m_keyChain(configBundle.keyChain)
  , m_nodeWatcher(nodeWatcher)
  , m_rng(ndn::random::getRandomNumberEngine())
{
  NDN_LOG_INFO("构造 Master");

  // Initialize bucket list
  for (unsigned int i = 0; i < NUM_BUCKETS; i++)
    m_buckets.push_back(Bucket(i));

  // Initialize SVS
  m_svs = std::make_unique<ndn::svs::SVSync>(
    m_syncPrefix, MASTER_PREFIX, m_face, std::bind(&Master::updateCallback, this, _1));

  // Wait 3s before initialization
  m_scheduler.schedule(ndn::time::milliseconds(3000), [this] { initialize(); });
}

void
Master::initialize()
{
  auto nodeList = m_nodeWatcher.getNodeList();

  if (nodeList.size() < NUM_REPLICA)
  {
    NDN_LOG_TRACE("节点数量不足，Master 不会初始化，需至少 " << NUM_REPLICA << " 个节点");
    m_scheduler.schedule(ndn::time::milliseconds(1000), [this] { initialize(); });
    return;
  }

  NDN_LOG_DEBUG("初始化 Master");

  m_initialized = true;

  auction();
}

void
Master::auction()
{
  if (!m_currentAuctionId)
  {
    for (unsigned int i = 0; i < m_buckets.size(); i++)
    {
      Bucket& b = m_buckets[i];
      if (b.confirmedHosts.size() == 0)
      {
        auction(i);
        break;
      }
    }
  }
  else
  {
    if (m_currentAuctionTime > AUCTION_TIME_LIMIT)
    {
      // Timeout
      m_currentAuctionId = 0;
    }
    m_currentAuctionTime++;
  }

  m_auctionRecheckEvent = m_scheduler.schedule(ndn::time::milliseconds(1000), [this] { auction(); });
}

void
Master::auction(unsigned int id)
{
  m_currentAuctionTime = 0;
  m_currentAuctionId = m_rng();
  m_currentAuctionBucketId = id;
  NDN_LOG_INFO("开始拍卖 bucket #" << m_currentAuctionBucketId << "，AID " << m_currentAuctionId);

  m_currentAuctionBids.clear();
  m_currentAuctionNumBidsExpected = m_nodeWatcher.getNodeList().size();
  m_buckets[m_currentAuctionBucketId].pendingHosts.clear();

  auto msg = newMsg(AuctionMessage::Type::Auction);
  m_svs->publishData(msg.wireEncode(), ndn::time::milliseconds(1000));
}

void
Master::updateCallback(const std::vector<ndn::svs::MissingDataInfo>& missingInfo)
{
  if (!m_initialized) return;

  for (const auto m : missingInfo) {
    for (ndn::svs::SeqNo i = m.low; i <= m.high; i++) {
      m_svs->fetchData(m.nodeId, i, std::bind(&Master::processMessage, this, m.nodeId, _1), 10);
    }
  }
}

void
Master::processMessage(const ndn::Name& sender, const ndn::Data& data)
{
  AuctionMessage msg;
  msg.wireDecode(data.getContent().blockFromValue());

  // Unknown auction
  if (msg.auctionId != m_currentAuctionId || msg.bucketId != m_currentAuctionBucketId)
    return;

  switch (msg.messageType)
  {
    case AuctionMessage::Type::Bid:
    {
      // Check for duplicate bids
      for (const Bid& bid : m_currentAuctionBids)
        if (bid.bidder == sender)
          return;

      // Count current bid
      m_currentAuctionBids.push_back(Bid { sender, msg.bidAmount });

      NDN_LOG_DEBUG("接收竞标来自 " << sender << "，bucket #" << msg.bucketId <<
                    "，AID " << msg.auctionId << "，出价 $" << msg.bidAmount);

      // Check if we got all bids
      if (m_currentAuctionBids.size() == m_currentAuctionNumBidsExpected)
        declareAuctionWinners();

      break;
    }

    case AuctionMessage::Type::WinAck:
    {
      NDN_LOG_TRACE("接收胜利确认来自 " << sender << "，bucket #" << msg.bucketId <<
                    "，AID " << msg.auctionId << "，出价 $" << msg.bidAmount);

      // Add to confirmed hosts if pending
      auto& m = m_buckets[m_currentAuctionBucketId].pendingHosts;
      if (m.count(sender))
      {
        m.erase(sender);
        m_buckets[m_currentAuctionBucketId].confirmedHosts[sender] = 1;
      }

      // Check if all pending are confirmed now
      if (m_buckets[m_currentAuctionBucketId].pendingHosts.size() == 0)
        endAuction();

      break;
    }

    default:
      return;
  }
}

void
Master::declareAuctionWinners()
{
  std::sort(m_currentAuctionBids.begin(), m_currentAuctionBids.end());

  for (int i = m_currentAuctionBids.size() - 1; i >= 0; i--)
  {
    const Bid& bid = m_currentAuctionBids[i];

    NDN_LOG_INFO(bid.bidder << " 赢得 bucket #" << m_currentAuctionBucketId << "，出价 " << bid.amount);

    m_buckets[m_currentAuctionBucketId].pendingHosts[bid.bidder] = 1;

    // Inform the winner
    auto msg = newMsg(AuctionMessage::Type::Win);
    msg.winner = bid.bidder;
    m_svs->publishData(msg.wireEncode(), ndn::time::milliseconds(1000));

    if (m_buckets[m_currentAuctionBucketId].pendingHosts.size() >= NUM_REPLICA)
    {
      break;
    }
  }

  if (m_buckets[m_currentAuctionBucketId].pendingHosts.size() == 0)
    endAuction();
}

void
Master::endAuction()
{
  auto msg = newMsg(AuctionMessage::Type::AuctionEnd);
  for (const auto& n : m_buckets[m_currentAuctionBucketId].confirmedHosts)
    msg.winnerList.push_back(n.first);
  m_svs->publishData(msg.wireEncode(), ndn::time::milliseconds(1000));
  NDN_LOG_INFO("拍卖结束");
  m_currentAuctionId = 0;
  auction();
}

} // namespace kua