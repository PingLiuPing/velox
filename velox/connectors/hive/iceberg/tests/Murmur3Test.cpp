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

#include "velox/connectors/hive/iceberg/Murmur3.h"
#include <gtest/gtest.h>
#include "folly/Random.h"

namespace facebook::velox::connectors::hive::iceberg {
class Murmur3HashTest : public ::testing::Test {
 public:
  void SetUp() override {
    rng_.seed(1);
  }
  void TearDown() override {}
  // Little-endian
  static std::vector<uint8_t> toBytes(uint64_t value) {
    std::vector<uint8_t> bytes;
    bytes.reserve(sizeof(uint64_t));
    for (int32_t i = 0; i < sizeof(uint64_t); ++i) {
      bytes[i] = static_cast<uint8_t>((value >> (8 * i)) & 0xFF);
    }
    return bytes;
  }

 protected:
  folly::Random::DefaultGenerator rng_;
};

TEST_F(Murmur3HashTest, HashEmptyString) {
  const uint32_t hash = Murmur3_32::hash("");
  EXPECT_EQ(hash, 0);
}

TEST_F(Murmur3HashTest, HashString) {
  const uint32_t hash = Murmur3_32::hash("iceberg");
  EXPECT_EQ(hash, 1210000089U);
}

TEST_F(Murmur3HashTest, HashInteger) {
  const uint32_t hash = Murmur3_32::hash(34);
  EXPECT_EQ(hash, 2017239379U);
}

TEST_F(Murmur3HashTest, HashTrue) {
  const uint32_t hash = Murmur3_32::hash(1);
  EXPECT_EQ(hash, 1392991556U);
}

TEST_F(Murmur3HashTest, HashDate) {
  const uint32_t hash = Murmur3_32::hash(17486);
  EXPECT_EQ(hash, static_cast<uint32_t>(-653330422));
}

TEST_F(Murmur3HashTest, HashLong) {
  const uint32_t hash = Murmur3_32::hash(static_cast<int64_t>(34));
  EXPECT_EQ(hash, 2017239379U);
}

TEST_F(Murmur3HashTest, HashDecimal) {
  const uint32_t hash = Murmur3_32::hashDecimal(1420);
  EXPECT_EQ(hash, static_cast<uint32_t>(-500754589));
}

TEST_F(Murmur3HashTest, HashBinary) {
  const uint8_t* bytes = new uint8_t[4]{0x00, 0x01, 0x02, 0x03};
  const uint32_t hash = Murmur3_32::hash(bytes, 4, 0);
  EXPECT_EQ(hash, static_cast<uint32_t>(-188683207));
}

// 2017-11-16T22:31:08
TEST_F(Murmur3HashTest, HashTimestamp) {
  const uint32_t hash =
      Murmur3_32::hash(static_cast<uint64_t>(1510871468000000L));
  EXPECT_EQ(hash, static_cast<uint32_t>(-2047944441));
}

TEST_F(Murmur3HashTest, HashIntegerAndBytes) {
  const uint32_t number = folly::Random::rand32(rng_);
  const uint32_t hashOfInteger = Murmur3_32::hash(number);
  const uint32_t hashOfBytes = Murmur3_32::hash(toBytes(number).data(), 8);
  EXPECT_EQ(hashOfInteger, hashOfBytes);
}

TEST_F(Murmur3HashTest, HashLongAndBytes) {
  const uint64_t number = folly::Random::rand64(rng_);
  const uint32_t hashOfLong = Murmur3_32::hash(number);
  const uint32_t hashOfBytes = Murmur3_32::hash(toBytes(number).data(), 8);
  EXPECT_EQ(hashOfLong, hashOfBytes);
}
} // namespace facebook::velox::connectors::hive::iceberg
