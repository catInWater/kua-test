#pragma once

namespace kua {

enum CommandCodes
{
  INSERT          = 0b00000001,
  NO_REPLICATE    = 0b00000010,
  IS_RANGE        = 0b00000100,
  MIGRATE         = 0b00001000,
  FETCH           = 0b10000000,
  KV_PUT          = 0x00010000,
  KV_GET          = 0x00020000,
  KV_LIST         = 0x00040000,
};

} // namespace kua