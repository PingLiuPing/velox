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

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include "velox/common/base/BitUtil.h"
#include "velox/common/base/Exceptions.h"
#include "velox/common/base/Portability.h"
#include "velox/common/memory/MemoryPool.h"
#include "velox/exec/SpillStats.h"

namespace facebook::velox::connector::hive {

/// Captures the file-system level configuration for a single logical writer.
/// A logical writer targets one partition/bucket combination and may emit
/// multiple files when rotation is enabled.
class WriterParameters {
 public:
  /// Controls how the coordinator should publish the files produced by a
  /// writer when the task completes.
  enum class UpdateMode {
    kNew,
    kOverwrite,
    kAppend,
  };

  /// Creates the immutable write parameters for a logical writer.
  WriterParameters(
      UpdateMode updateMode,
      std::optional<std::string> partitionName,
      std::string targetFileName,
      std::string targetDirectory,
      std::optional<std::string> writeFileName = std::nullopt,
      std::optional<std::string> writeDirectory = std::nullopt)
      : updateMode_(updateMode),
        partitionName_(std::move(partitionName)),
        targetFileName_(std::move(targetFileName)),
        targetDirectory_(std::move(targetDirectory)),
        writeFileName_(writeFileName.value_or(targetFileName_)),
        writeDirectory_(writeDirectory.value_or(targetDirectory_)) {}

  /// Returns how the coordinator should apply this writer's output.
  UpdateMode updateMode() const {
    return updateMode_;
  }

  /// Converts an update mode to the string expected by commit messages.
  static std::string updateModeToString(UpdateMode updateMode) {
    switch (updateMode) {
      case UpdateMode::kNew:
        return "NEW";
      case UpdateMode::kOverwrite:
        return "OVERWRITE";
      case UpdateMode::kAppend:
        return "APPEND";
      default:
        VELOX_UNSUPPORTED("Unsupported update mode.");
    }
  }

  /// Returns the partition directory fragment for this writer, if any.
  const std::optional<std::string>& partitionName() const {
    return partitionName_;
  }

  /// Returns the final file name after commit.
  const std::string& targetFileName() const {
    return targetFileName_;
  }

  /// Returns the temporary file name used while the task is writing.
  const std::string& writeFileName() const {
    return writeFileName_;
  }

  /// Returns the final output directory for committed files.
  const std::string& targetDirectory() const {
    return targetDirectory_;
  }

  /// Returns the staging directory where the task writes its files.
  const std::string& writeDirectory() const {
    return writeDirectory_;
  }

 private:
  // Controls how the coordinator should merge this writer's files into the
  // destination table or partition.
  const UpdateMode updateMode_;
  // Stores the partition path fragment for this writer.
  const std::optional<std::string> partitionName_;
  // Stores the final file name that should appear after commit.
  const std::string targetFileName_;
  // Stores the final destination directory for committed files.
  const std::string targetDirectory_;
  // Stores the staging file name used while the task is writing.
  const std::string writeFileName_;
  // Stores the staging directory used while the task is writing.
  const std::string writeDirectory_;
};

/// Describes a single physical file emitted by a logical writer.
struct FileInfo {
  // Stores the temporary file name used in the staging directory.
  std::string writeFileName;
  // Stores the final file name after commit completes.
  std::string targetFileName;
  // Stores the on-disk size of the completed file in bytes.
  uint64_t fileSize{0};
  // Stores the number of rows written into the completed file.
  uint64_t numRows{0};
};

/// Tracks mutable execution state for one logical writer across all of its
/// rotated files.
struct WriterInfo {
  /// Creates the per-writer execution state shared by the routing and rolling
  /// writer layers.
  WriterInfo(
      WriterParameters parameters,
      std::shared_ptr<memory::MemoryPool> writerPool,
      std::shared_ptr<memory::MemoryPool> sinkPool,
      std::shared_ptr<memory::MemoryPool> sortPool)
      : writerParameters(std::move(parameters)),
        nonReclaimableSectionHolder(new tsan_atomic<bool>(false)),
        spillStats(std::make_unique<exec::SpillStats>()),
        writerPool(std::move(writerPool)),
        sinkPool(std::move(sinkPool)),
        sortPool(std::move(sortPool)) {}

  // Stores the immutable file-system configuration for this logical writer.
  const WriterParameters writerParameters;
  // Marks critical sections where writer memory cannot be reclaimed safely.
  const std::unique_ptr<tsan_atomic<bool>> nonReclaimableSectionHolder;
  // Accumulates spill statistics produced by an optional sorting layer.
  const std::unique_ptr<exec::SpillStats> spillStats;
  // Owns the top-level memory pool for the format writer.
  const std::shared_ptr<memory::MemoryPool> writerPool;
  // Owns the file-sink memory pool used by the underlying file writer.
  const std::shared_ptr<memory::MemoryPool> sinkPool;
  // Owns the sorting memory pool when sorted writes are enabled.
  const std::shared_ptr<memory::MemoryPool> sortPool;
  // Counts all rows written by this logical writer across every file.
  uint64_t numWrittenRows = 0;
  // Counts rows written into the currently open file.
  uint64_t currentFileWrittenRows{0};
  // Tracks the in-memory input size seen by this writer across all writes.
  uint64_t inputSizeInBytes = 0;
  // Tracks how many files have already been finalized for this writer.
  uint32_t fileSequenceNumber{0};
  // Stores metadata for every completed file, including rotated files.
  std::vector<FileInfo> writtenFiles;
  // Stores the cumulative bytes written before the current file started.
  uint64_t cumulativeWrittenBytes{0};
  // Stores the current staging file name for the active file.
  std::string currentWriteFileName;
  // Stores the current committed file name for the active file.
  std::string currentTargetFileName;
};

/// Identifies a logical writer by partition id, bucket id, or both.
struct WriterId {
  std::optional<uint32_t> partitionId{std::nullopt};
  std::optional<uint32_t> bucketId{std::nullopt};

  /// Creates an empty writer id that can be populated later.
  WriterId() = default;

  /// Creates an identifier for a partition writer, bucket writer, or both.
  WriterId(
      std::optional<uint32_t> partitionId,
      std::optional<uint32_t> bucketId = std::nullopt)
      : partitionId(partitionId), bucketId(bucketId) {}

  /// Returns the canonical identifier for the single writer used by
  /// unpartitioned, unbucketed tables.
  static const WriterId& unpartitionedId();

  /// Formats the identifier for logging and memory-pool naming.
  std::string toString() const;

  /// Compares two writer identifiers by their partition and bucket keys.
  bool operator==(const WriterId& other) const {
    return std::tie(partitionId, bucketId) ==
        std::tie(other.partitionId, other.bucketId);
  }
};

/// Hashes a writer identifier for use in hash maps.
struct WriterIdHasher {
  /// Returns a stable hash derived from the partition and bucket ids.
  std::size_t operator()(const WriterId& id) const {
    return bits::hashMix(
        id.partitionId.value_or(std::numeric_limits<uint32_t>::max()),
        id.bucketId.value_or(std::numeric_limits<uint32_t>::max()));
  }
};

/// Compares two writer identifiers for equality inside hash maps.
struct WriterIdEq {
  /// Returns true when both writer identifiers target the same partition and
  /// bucket combination.
  bool operator()(const WriterId& lhs, const WriterId& rhs) const {
    return lhs == rhs;
  }
};

} // namespace facebook::velox::connector::hive
