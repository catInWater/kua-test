#include <iostream>
#include <string>
#include <ndn-cxx/util/logger.hpp>

#include "config-bundle.hpp"
#include "node-watcher.hpp"
#include "bidder.hpp"
#include "nlsr.hpp"

NDN_LOG_INIT(kua.main);

int
main(int argc, char *argv[])
{
  if (argc < 3)
  {
    std::cerr << "用法: kua <kua-prefix> <node-prefix>" << std::endl;
    exit(1);
  }

  // Get arguments
  const ndn::Name kuaPrefix(argv[1]);
  const ndn::Name nodePrefix(argv[2]);

  // Start face and keychain
  ndn::Face face;
  ndn::KeyChain keyChain;
  kua::NLSR nlsr(keyChain, face);

  // Create common bundle
  kua::ConfigBundle configBundle { kuaPrefix, nodePrefix, face, keyChain, false };

  // Start components
  kua::NodeWatcher nodeWatcher(configBundle);
  kua::Bidder bidder(configBundle, nodeWatcher);

  // Advertise basic prefixes
  nlsr.advertise(nodePrefix);
  nlsr.advertise(ndn::Name(kuaPrefix).append("sync").append("health"));

  // No centralized master: consistent hashing assigns buckets to nodes.

  // Infinite loop
  face.processEvents();
}