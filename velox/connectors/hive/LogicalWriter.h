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
#pragma once

#include <memory>

#include "velox/common/io/IoStatistics.h"
#include "velox/dwio/common/Writer.h"

namespace facebook::velox::connector::hive {

struct WriterInfo;

/// Represents a logical writer owned by a partition-routing strategy.
/// A logical writer may wrap multiple physical file writers over time because
/// of file rotation, but it exposes a stable writer id, shared writer state,
/// and aggregated IO statistics to higher layers.
class LogicalWriter : public dwio::common::Writer {
 public:
  /// Destroys the logical writer after any owned writer stack has been
  /// released.
  ~LogicalWriter() override = default;

  /// Returns the shared mutable state tracked for this logical writer.
  virtual const std::shared_ptr<WriterInfo>& writerInfo() const = 0;

  /// Returns the IO statistics accumulated across all files produced by this
  /// logical writer.
  virtual io::IoStatistics* ioStats() const = 0;
};

} // namespace facebook::velox::connector::hive
