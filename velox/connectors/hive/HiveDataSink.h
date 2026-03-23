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

#include "velox/common/compression/Compression.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HivePartitionName.h"
#include "velox/connectors/hive/PartitionIdGenerator.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/connectors/hive/WriterInfo.h"
#include "velox/dwio/common/Options.h"

namespace facebook::velox::dwio::common {
class FileMetadata;
} // namespace facebook::velox::dwio::common

namespace facebook::velox::connector::hive {

class LocationHandle;
using LocationHandlePtr = std::shared_ptr<const LocationHandle>;

/// Location related properties of the Hive table to be written.
class LocationHandle : public ISerializable {
 public:
  enum class TableType {
    /// Write to a new table to be created.
    kNew,
    /// Write to an existing table.
    kExisting,
  };

  LocationHandle(
      std::string targetPath,
      std::string writePath,
      TableType tableType,
      std::string targetFileName = "")
      : targetPath_(std::move(targetPath)),
        targetFileName_(std::move(targetFileName)),
        writePath_(std::move(writePath)),
        tableType_(tableType) {}

  const std::string& targetPath() const {
    return targetPath_;
  }

  const std::string& targetFileName() const {
    return targetFileName_;
  }

  const std::string& writePath() const {
    return writePath_;
  }

  TableType tableType() const {
    return tableType_;
  }

  std::string toString() const;

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static LocationHandlePtr create(const folly::dynamic& obj);

  static const std::string tableTypeName(LocationHandle::TableType type);

  static LocationHandle::TableType tableTypeFromName(const std::string& name);

 private:
  // Target directory path.
  const std::string targetPath_;
  // If non-empty, use this name instead of generating our own.
  const std::string targetFileName_;
  // Staging directory path.
  const std::string writePath_;
  // Whether the table to be written is new, already existing or temporary.
  const TableType tableType_;
};

class HiveSortingColumn : public ISerializable {
 public:
  HiveSortingColumn(
      const std::string& sortColumn,
      const core::SortOrder& sortOrder);

  const std::string& sortColumn() const {
    return sortColumn_;
  }

  core::SortOrder sortOrder() const {
    return sortOrder_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<HiveSortingColumn> deserialize(
      const folly::dynamic& obj,
      void* context);

  std::string toString() const;

  static void registerSerDe();

 private:
  const std::string sortColumn_;
  const core::SortOrder sortOrder_;
};

class HiveBucketProperty : public ISerializable {
 public:
  enum class Kind { kHiveCompatible, kPrestoNative };

  HiveBucketProperty(
      Kind kind,
      int32_t bucketCount,
      const std::vector<std::string>& bucketedBy,
      const std::vector<TypePtr>& bucketedTypes,
      const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy);

  Kind kind() const {
    return kind_;
  }

  static std::string kindString(Kind kind);

  /// Returns the number of bucket count.
  int32_t bucketCount() const {
    return bucketCount_;
  }

  /// Returns the bucketed by column names.
  const std::vector<std::string>& bucketedBy() const {
    return bucketedBy_;
  }

  /// Returns the bucketed by column types.
  const std::vector<TypePtr>& bucketedTypes() const {
    return bucketTypes_;
  }

  /// Returns the hive sorting columns if not empty.
  const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy()
      const {
    return sortedBy_;
  }

  folly::dynamic serialize() const override;

  static std::shared_ptr<HiveBucketProperty> deserialize(
      const folly::dynamic& obj,
      void* context);

  bool operator==(const HiveBucketProperty& other) const {
    return true;
  }

  static void registerSerDe();

  std::string toString() const;

 private:
  void validate() const;

  const Kind kind_;
  const int32_t bucketCount_;
  const std::vector<std::string> bucketedBy_;
  const std::vector<TypePtr> bucketTypes_;
  const std::vector<std::shared_ptr<const HiveSortingColumn>> sortedBy_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    HiveBucketProperty::Kind kind) {
  os << HiveBucketProperty::kindString(kind);
  return os;
}

class HiveInsertTableHandle;
using HiveInsertTableHandlePtr = std::shared_ptr<HiveInsertTableHandle>;

class FileNameGenerator : public ISerializable {
 public:
  virtual ~FileNameGenerator() = default;

  virtual std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const = 0;

  virtual std::string toString() const = 0;
};

class HiveInsertFileNameGenerator : public FileNameGenerator {
 public:
  HiveInsertFileNameGenerator() {}

  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      bool commitRequired) const override;

  /// Version of file generation that takes hiveConfig into account when
  /// generating file names
  std::pair<std::string, std::string> gen(
      std::optional<uint32_t> bucketId,
      const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx& connectorQueryCtx,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      bool commitRequired) const;

  static void registerSerDe();

  folly::dynamic serialize() const override;

  static std::shared_ptr<HiveInsertFileNameGenerator> deserialize(
      const folly::dynamic& obj,
      void* context);

  std::string toString() const override;

  /// Replaces potentially unsafe characters in a file name with underscores
  static void sanitizeFileName(std::string& name);
};

/// Represents a request for Hive write.
class HiveInsertTableHandle : public ConnectorInsertTableHandle {
 public:
  HiveInsertTableHandle(
      std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns,
      std::shared_ptr<const LocationHandle> locationHandle,
      dwio::common::FileFormat storageFormat = dwio::common::FileFormat::DWRF,
      std::shared_ptr<const HiveBucketProperty> bucketProperty = nullptr,
      std::optional<common::CompressionKind> compressionKind = {},
      const std::unordered_map<std::string, std::string>& serdeParameters = {},
      const std::shared_ptr<dwio::common::WriterOptions>& writerOptions =
          nullptr,
      // When this option is set the HiveDataSink will always write a file even
      // if there's no data. This is useful when the table is bucketed, but the
      // engine handles ensuring a 1 to 1 mapping from task to bucket.
      const bool ensureFiles = false,
      std::shared_ptr<const FileNameGenerator> fileNameGenerator =
          std::make_shared<const HiveInsertFileNameGenerator>());

  virtual ~HiveInsertTableHandle() = default;

  const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns()
      const {
    return inputColumns_;
  }

  const std::shared_ptr<const LocationHandle>& locationHandle() const {
    return locationHandle_;
  }

  std::optional<common::CompressionKind> compressionKind() const {
    return compressionKind_;
  }

  dwio::common::FileFormat storageFormat() const {
    return storageFormat_;
  }

  const std::unordered_map<std::string, std::string>& serdeParameters() const {
    return serdeParameters_;
  }

  const std::shared_ptr<dwio::common::WriterOptions>& writerOptions() const {
    return writerOptions_;
  }

  bool ensureFiles() const {
    return ensureFiles_;
  }

  const std::shared_ptr<const FileNameGenerator>& fileNameGenerator() const {
    return fileNameGenerator_;
  }

  bool supportsMultiThreading() const override {
    return true;
  }

  bool isPartitioned() const;

  bool isBucketed() const;

  const HiveBucketProperty* bucketProperty() const;

  bool isExistingTable() const;

  /// Returns a subset of column indices corresponding to partition keys.
  const std::vector<column_index_t>& partitionChannels() const {
    return partitionChannels_;
  }

  /// Returns the column indices of non-partition data columns.
  const std::vector<column_index_t>& nonPartitionChannels() const {
    return nonPartitionChannels_;
  }

  folly::dynamic serialize() const override;

  static HiveInsertTableHandlePtr create(const folly::dynamic& obj);

  static void registerSerDe();

  std::string toString() const override;

 protected:
  const std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns_;
  const std::shared_ptr<const LocationHandle> locationHandle_;

 private:
  const dwio::common::FileFormat storageFormat_;
  const std::shared_ptr<const HiveBucketProperty> bucketProperty_;
  const std::optional<common::CompressionKind> compressionKind_;
  const std::unordered_map<std::string, std::string> serdeParameters_;
  const std::shared_ptr<dwio::common::WriterOptions> writerOptions_;
  const bool ensureFiles_;
  const std::shared_ptr<const FileNameGenerator> fileNameGenerator_;
  const std::vector<column_index_t> partitionChannels_;
  const std::vector<column_index_t> nonPartitionChannels_;
};

class PartitionWriter;
class LogicalWriterFactory;

/// Orchestrates a table write by computing partition and bucket ids, creating
/// logical writers through a routing strategy, and producing the final commit
/// protocol payload consumed by the coordinator.
class HiveDataSink : public DataSink {
 public:
  /// The list of runtime stats reported by hive data sink
  static constexpr const char* kEarlyFlushedRawBytes = "earlyFlushedRawBytes";

  /// Releases the sink after all owned writers and routing state have been
  /// destroyed.
  ~HiveDataSink() override;

  /// Defines the execution states of a hive data sink running internally.
  enum class State {
    /// The data sink accepts new append data in this state.
    kRunning = 0,
    /// The data sink flushes any buffered data to the underlying file writer
    /// but no more data can be appended.
    kFinishing = 1,
    /// The data sink is aborted on error and no more data can be appended.
    kAborted = 2,
    /// The data sink is closed on error and no more data can be appended.
    kClosed = 3
  };
  static std::string stateString(State state);

  /// Creates a HiveDataSink for writing data to Hive table files.
  ///
  /// @param inputType The schema of input data rows to be written.
  /// @param insertTableHandle Metadata about the table write operation,
  /// including storage format, compression, bucketing, and partitioning
  /// configuration.
  /// @param connectorQueryCtx Query context with session properties, memory
  /// pools, and spill configuration.
  /// @param commitStrategy Strategy for committing written data (kNoCommit or
  /// kTaskCommit).
  /// @param hiveConfig Hive connector configuration.
  HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const HiveConfig>& hiveConfig);

  /// Constructor with explicit bucketing and partitioning parameters.
  ///
  /// @param inputType The schema of input data rows to be written.
  /// @param insertTableHandle Metadata about the table write operation,
  /// including storage format, compression, location, and serialization
  /// parameters.
  /// @param connectorQueryCtx Query context with session properties, memory
  /// pools, and spill configuration.
  /// @param commitStrategy Strategy for committing written data (kNoCommit or
  /// kTaskCommit). Determines whether temporary files need to be renamed on
  /// commit.
  /// @param hiveConfig Hive connector configuration with settings for max
  /// partitions, bucketing limits etc.
  /// @param bucketCount Number of buckets for bucketed tables (0 if not
  /// bucketed). Must be less than the configured max bucket count.
  /// @param bucketFunction Function to compute bucket IDs from row data
  /// (nullptr if not bucketed). Used to distribute rows across buckets.
  /// @param partitionChannels Column indices used for partitioning (empty if
  /// not partitioned). These columns are extracted to determine partition
  /// directories.
  /// @param dataChannels Column indices for the actual data columns to be
  /// written.
  /// @param partitionIdGenerator Generates partition IDs from partition column
  /// values (nullptr if not partitioned). Compute partition key combinations to
  /// unique IDs.
  HiveDataSink(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      CommitStrategy commitStrategy,
      const std::shared_ptr<const HiveConfig>& hiveConfig,
      uint32_t bucketCount,
      std::unique_ptr<core::PartitionFunction> bucketFunction,
      const std::vector<column_index_t>& partitionChannels,
      const std::vector<column_index_t>& dataChannels,
      std::unique_ptr<PartitionIdGenerator> partitionIdGenerator);

  /// Appends a batch of input rows to the sink, routing each row to the
  /// correct logical writer.
  void appendData(RowVectorPtr input) override;

  /// Finishes any buffered writer state and returns false when a writer yields
  /// and requires another finish() call.
  bool finish() override;

  /// Returns aggregate write statistics collected by all logical writers.
  Stats stats() const override;

  /// Returns runtime statistics reported by the underlying file system layer.
  std::unordered_map<std::string, RuntimeCounter> runtimeStats() const override;

  /// Closes the sink, finalizes all files, and returns the commit protocol
  /// payload for the coordinator.
  std::vector<std::string> close() override;

  /// Aborts the sink and drops any uncommitted writer state.
  void abort() override;

  /// Returns true when the sink can reclaim memory from its format writers.
  bool canReclaim() const;

 protected:
  // Validates the state transition from 'oldState' to 'newState'.
  void checkStateTransition(State oldState, State newState);
  void setState(State newState);

  // Generates commit messages for all writers containing metadata about written
  // files. Creates a JSON object for each writer with partition name,
  // file paths, file names, data sizes, and row counts. This metadata is used
  // by the coordinator to commit the transaction and update the metastore.
  //
  // @return Vector of JSON strings, one per writer.
  virtual std::vector<std::string> commitMessage() const;

  // Returns true if the table is partitioned.
  FOLLY_ALWAYS_INLINE bool isPartitioned() const {
    return partitionIdGenerator_ != nullptr;
  }

  // Returns true if the table is bucketed.
  FOLLY_ALWAYS_INLINE bool isBucketed() const {
    return bucketCount_ != 0;
  }

  FOLLY_ALWAYS_INLINE bool isCommitRequired() const {
    return commitStrategy_ != CommitStrategy::kNoCommit;
  }

  // Compute the partition id and bucket id for each row in 'input'.
  virtual void computePartitionAndBucketIds(const RowVectorPtr& input);

  // Applies format-specific tweaks to a fully prepared per-writer options
  // object before the leaf format writer is created.
  virtual void customizeWriterOptions(
      const WriterInfo& writerInfo,
      const std::shared_ptr<dwio::common::WriterOptions>& options) const;

  // Returns the Hive partition directory name for the given partition ID.
  // Converts the partition values associated with the partition ID into a
  // Hive-formatted directory path. Returns std::nullopt if the table is
  // unpartitioned. Should be called only when writing to a partitioned table.
  virtual std::string getPartitionName(uint32_t partitionId) const;

  // Lifecycle hooks for format-specific extensions.
  virtual void onWriterCreated(const WriterId& id, uint32_t writerIndex);
  virtual void onFileClosed(
      uint32_t writerIndex,
      std::optional<FileInfo> fileInfo,
      std::unique_ptr<dwio::common::FileMetadata> metadata);

  WriterParameters::UpdateMode getUpdateMode() const;

  FOLLY_ALWAYS_INLINE void checkRunning() const {
    VELOX_CHECK_EQ(state_, State::kRunning, "Hive data sink is not running");
  }

  void closeInternal();

  // Aggregates file-system level runtime statistics across all writers.
  std::unique_ptr<IoStats> fileSystemStats_;

  // Stores the full input schema received by appendData().
  const RowTypePtr inputType_;
  // Stores immutable table write configuration supplied by the connector.
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  // Provides query-scoped services such as memory pools and session settings.
  const ConnectorQueryCtx* const connectorQueryCtx_;
  // Controls whether the sink writes directly to the target path or stages
  // files for task commit.
  const CommitStrategy commitStrategy_;
  // Provides Hive-specific session and connector configuration.
  const std::shared_ptr<const HiveConfig> hiveConfig_;
  // Stores how the coordinator should publish files for this table write.
  const WriterParameters::UpdateMode updateMode_;
  // Limits how many logical writers may be opened concurrently.
  const uint32_t maxOpenWriters_;
  // Stores the input-channel indices used to compute partition ids.
  const std::vector<column_index_t> partitionChannels_;
  // Generates stable partition ids from partition-column values.
  const std::unique_ptr<PartitionIdGenerator> partitionIdGenerator_;
  // Stores the input-channel indices that belong to the physical data file.
  const std::vector<column_index_t> dataChannels_;
  // Stores the configured bucket count, or zero when bucketing is disabled.
  const int32_t bucketCount_{0};
  // Computes bucket ids for each input row when bucketing is enabled.
  const std::unique_ptr<core::PartitionFunction> bucketFunction_;
  // Controls whether generated partition path keys are lower-cased.
  const bool partitionKeyAsLowerCase_;

  // Tracks the sink lifecycle state across append, finish, close, and abort.
  State state_{State::kRunning};

  // Owns the writer-behavior factory that assembles logical writer stacks.
  std::unique_ptr<LogicalWriterFactory> logicalWriterFactory_;
  // Owns the routing strategy responsible for mapping rows to logical writers.
  std::unique_ptr<PartitionWriter> partitionWriter_;

  // Stores the partition id for each row in the current input batch.
  raw_vector<uint64_t> partitionIds_;

  // Stores the bucket id for each row in the current input batch.
  std::vector<uint32_t> bucketIds_;
};

FOLLY_ALWAYS_INLINE std::ostream& operator<<(
    std::ostream& os,
    HiveDataSink::State state) {
  os << HiveDataSink::stateString(state);
  return os;
}
} // namespace facebook::velox::connector::hive

template <>
struct fmt::formatter<facebook::velox::connector::hive::HiveDataSink::State>
    : formatter<std::string> {
  auto format(
      facebook::velox::connector::hive::HiveDataSink::State s,
      format_context& ctx) const {
    return formatter<std::string>::format(
        facebook::velox::connector::hive::HiveDataSink::stateString(s), ctx);
  }
};

template <>
struct fmt::formatter<
    facebook::velox::connector::hive::LocationHandle::TableType>
    : formatter<int> {
  auto format(
      facebook::velox::connector::hive::LocationHandle::TableType s,
      format_context& ctx) const {
    return formatter<int>::format(static_cast<int>(s), ctx);
  }
};
