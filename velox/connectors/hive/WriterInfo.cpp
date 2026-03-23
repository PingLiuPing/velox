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

#include "velox/connectors/hive/WriterInfo.h"

namespace facebook::velox::connector::hive {

const WriterId& WriterId::unpartitionedId() {
  static const WriterId writerId{0};
  return writerId;
}

std::string WriterId::toString() const {
  if (partitionId.has_value() && bucketId.has_value()) {
    return fmt::format("part[{}.{}]", partitionId.value(), bucketId.value());
  }

  if (partitionId.has_value() && !bucketId.has_value()) {
    return fmt::format("part[{}]", partitionId.value());
  }

  if (!partitionId.has_value() && bucketId.has_value()) {
    return fmt::format("bucket[{}]", bucketId.value());
  }

  return "unpart";
}

} // namespace facebook::velox::connector::hive
