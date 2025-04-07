/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "Murmur3.h"
#include <cstddef>
#include <cstring>
#include "velox/velox/type/HugeInt.h"

namespace facebook::velox::connectors::hive::iceberg {

std::vector<uint8_t> Murmur3_32::intToMinimalBytes(int32_t value) {
  std::vector<uint8_t> bytes;
  const bool isNegative = (value < 0);

  for (int i = 3; i >= 0; --i) {
    uint8_t byte = static_cast<uint8_t>((value >> (i * 8)) & 0xFF);

    if (bytes.empty()) {
      // Skip leading 0x00 or 0xFF if they don’t affect sign
      if ((isNegative && byte == 0xFF && ((value >> ((i - 1) * 8)) & 0x80)) ||
          (!isNegative && byte == 0x00 &&
           ((value >> ((i - 1) * 8)) & 0x80) == 0)) {
        continue;
      }
    }
    bytes.push_back(byte);
  }

  if (!bytes.empty()) {
    uint8_t signBit = bytes[0] & 0x80;
    if ((isNegative && signBit == 0) || (!isNegative && signBit != 0)) {
      bytes.insert(bytes.begin(), isNegative ? 0xFF : 0x00);
    }
  } else {
    // value == 0
    bytes.push_back(0x00);
  }
  return bytes;
}

uint32_t
Murmur3_32::hash(const uint8_t* data, const size_t len, const uint32_t seed) {
  uint32_t h1 = seed;
  uint32_t k1{0};
  const size_t bsize = sizeof(k1);
  const size_t nblocks = len / bsize;

  // body
  for (size_t i = 0; i < nblocks; i++, data += bsize) {
    memcpy(&k1, data, bsize);

    k1 *= C1;
    k1 = ROTL32(k1, 15);
    k1 *= C2;

    h1 ^= k1;
    h1 = ROTL32(h1, 13);
    h1 = h1 * 5 + 0XE6546B64;
  }
  k1 = 0;
  // tail
  switch (len & 3) {
    case 3:
      k1 ^= (static_cast<uint32_t>(data[2])) << 16;
      [[fallthrough]];
    case 2:
      k1 ^= (static_cast<uint32_t>(data[1])) << 8;
      [[fallthrough]];
    case 1:
      k1 ^= data[0];
      k1 *= C1;
      k1 = ROTL32(k1, 15);
      k1 *= C2;
      h1 ^= k1;
  };

  // finalization
  h1 ^= static_cast<uint32_t>(len);
  FMIX32(h1);
  return h1;
}

uint32_t Murmur3_32::hash(
    const facebook::velox::StringView& value,
    const uint32_t seed) {
  return hash(
      reinterpret_cast<const uint8_t*>(value.data()), value.size(), seed);
}

uint32_t Murmur3_32::hashInt(uint32_t value, const uint32_t seed) {
  uint32_t k1 = mixK1(value);
  uint32_t h1 = mixH1(seed, k1);
  h1 ^= sizeof(uint32_t);
  return FMIX32(h1);
}
uint32_t Murmur3_32::hash(int32_t value, const uint32_t seed) {
  return hash(static_cast<uint64_t>(value), seed);
}

uint32_t Murmur3_32::hash(uint32_t value, const uint32_t seed) {
  return hash(static_cast<uint64_t>(value), seed);
}

uint32_t Murmur3_32::hash(int64_t value, const uint32_t seed) {
  return hash(static_cast<uint64_t>(value), seed);
}

uint32_t Murmur3_32::hash(uint64_t value, const uint32_t seed) {
  uint32_t h1 = seed;
  uint32_t low = static_cast<uint32_t>(value & 0xFFFFFFFF);
  uint32_t high = static_cast<uint32_t>((value >> 32) & 0xFFFFFFFF);

  uint32_t k1 = mixK1(low);
  h1 = mixH1(h1, k1);

  k1 = mixK1(high);
  h1 = mixH1(h1, k1);

  h1 ^= sizeof(uint64_t);
  FMIX32(h1);
  return h1;
}

uint32_t Murmur3_32::hash(folly::int128_t value, const uint32_t seed) {
  char buf[sizeof(folly::int128_t)] = {};
  HugeInt::serialize(value, buf);
  return hash(reinterpret_cast<const uint8_t*>(buf), sizeof(buf), seed);
}

uint32_t Murmur3_32::hashDecimal(uint32_t value, const uint32_t seed) {
  std::vector<uint8_t> bytes = intToMinimalBytes(value);
  return hash(bytes.data(), bytes.size(), seed);
}
} // namespace facebook::velox::connectors::hive::iceberg
