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

#include <functional>
#include <memory>
#include <optional>

#include "velox/common/base/CompareFlags.h"
#include "velox/common/file/FileSystems.h"
#include "velox/common/io/IoStatistics.h"
#include "velox/connectors/Connector.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/WriterInfo.h"
#include "velox/dwio/common/WriterFactory.h"

namespace facebook::velox::connector::hive {

class HiveInsertTableHandle;
class LogicalWriter;

/// Returns the RowType containing only the data columns identified by the
/// given channel indices.
RowTypePtr getNonPartitionTypes(
    const std::vector<column_index_t>& dataChannels,
    const RowTypePtr& inputType);

/// Creates the per-partition logical writer stack used by HiveDataSink.
///
/// Each call to createWriter() produces a RotationWriter that wraps one or
/// more format-specific leaf writers (DWRF, Parquet, or Nimble). The full
/// writer stack from outer to inner is:
///
///   RotationWriter -> [SortingWriter ->] FormatWriter
///
/// The optional SortingWriter layer is inserted when the table has a bucket
/// sort specification. RotationWriter handles file-size-based rotation for
/// unbucketed, unsorted writers, and single-file semantics otherwise.
///
/// The factory owns cross-writer concerns — memory pool creation, reclaimer
/// wiring, writer-option assembly, and file-path generation — so the data
/// sink can focus on table-level orchestration and commit semantics.
class LogicalWriterFactory {
 public:
  /// Applies format-specific tweaks to writer options after the factory has
  /// populated the common fields (schema, compression, memory pool, etc.).
  using WriterOptionsCustomizer = std::function<void(
      const WriterInfo&,
      const std::shared_ptr<dwio::common::WriterOptions>&)>;

  /// Computes the partition directory name for a logical writer, or nullopt
  /// for unpartitioned tables.
  using PartitionNameFactory =
      std::function<std::optional<std::string>(const WriterId&)>;

  /// Receives notification when a file is finalized so the data sink can
  /// collect format-specific metadata (e.g. Iceberg statistics).
  using FileClosedCallback = std::function<void(
      uint32_t,
      std::optional<FileInfo>,
      std::unique_ptr<dwio::common::FileMetadata>)>;

  /// Creates a factory bound to a single HiveDataSink instance.
  LogicalWriterFactory(
      RowTypePtr inputType,
      std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
      const ConnectorQueryCtx* connectorQueryCtx,
      std::shared_ptr<const HiveConfig> hiveConfig,
      WriterParameters::UpdateMode updateMode,
      bool commitRequired,
      const std::vector<column_index_t>& dataChannels,
      PartitionNameFactory partitionNameFactory,
      WriterOptionsCustomizer writerOptionsCustomizer,
      FileClosedCallback fileClosedCallback,
      IoStats* fileSystemStats);

  /// Creates a logical writer for the given partition/bucket identity and
  /// assigns it the specified stable writer index.
  std::unique_ptr<LogicalWriter> createWriter(
      const WriterId& id,
      uint32_t writerIndex);

  /// Returns true when the produced writers support memory reclaim.
  bool canReclaim() const {
    return canReclaim_;
  }

 private:
  // Creates a dedicated memory pool for one logical writer.
  std::shared_ptr<memory::MemoryPool> createWriterPool(
      const WriterId& writerId) const;

  // Installs memory reclaimers on the writer and sink pools.
  void setMemoryReclaimers(WriterInfo* writerInfo, io::IoStatistics* ioStats)
      const;

  // Creates the leaf format writer and optionally wraps it with a
  // SortingWriter for bucketed sorted output.
  std::unique_ptr<dwio::common::Writer> createFormatWriter(
      WriterInfo* writerInfo,
      io::IoStatistics* ioStats) const;

  // Assembles writer options from table handle defaults, session settings,
  // and the optional customizer callback.
  std::shared_ptr<dwio::common::WriterOptions> createWriterOptions(
      const WriterInfo& writerInfo) const;

  // Computes the write/target directories and file names for a writer.
  WriterParameters getWriterParameters(const WriterId& id) const;

  // Generates write and target file names based on the commit strategy and
  // optional bucket id.
  std::pair<std::string, std::string> getWriterFileNames(
      std::optional<uint32_t> bucketId) const;

  // Wraps the format writer in a SortingWriter when the table has a bucket
  // sort specification.
  std::unique_ptr<dwio::common::Writer> maybeCreateSortingWriter(
      WriterInfo* writerInfo,
      std::unique_ptr<dwio::common::Writer> writer) const;

  // Full input schema including both partition and data columns.
  const RowTypePtr inputType_;
  // Immutable table write configuration (format, compression, columns, etc.).
  const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle_;
  // Query-scoped services: memory pools, session settings, spill config.
  const ConnectorQueryCtx* const connectorQueryCtx_;
  // Hive-specific session and connector configuration.
  const std::shared_ptr<const HiveConfig> hiveConfig_;
  // How the coordinator should publish files (new, append, overwrite).
  const WriterParameters::UpdateMode updateMode_;
  // Whether file naming uses separate write and target names for atomic
  // task-level commit.
  const bool commitRequired_;
  // Computes partition directory names for partitioned writers.
  const PartitionNameFactory partitionNameFactory_;
  // Applies format-specific writer-option tweaks after common setup.
  const WriterOptionsCustomizer writerOptionsCustomizer_;
  // Receives notification when a file is finalized.
  const FileClosedCallback fileClosedCallback_;
  // Accumulates filesystem-level IO stats across all writers.
  IoStats* const fileSystemStats_;
  // Registered factory for creating leaf format writers (DWRF/Parquet/Nimble).
  const std::shared_ptr<dwio::common::WriterFactory> leafWriterFactory_;
  // Spill configuration for reclaimable writers, or nullptr if spill is
  // disabled.
  const common::SpillConfig* const spillConfig_;
  // Time slice limit in milliseconds for SortingWriter::finish().
  const uint64_t sortWriterFinishTimeSliceLimitMs_;
  // File size threshold that triggers RotationWriter to open a new file.
  const uint64_t maxTargetFileBytes_;
  // Whether the produced writers support memory reclaim.
  const bool canReclaim_;
  // Input-channel indices that map to the physical data file columns. For
  // Hive this excludes partition columns; for Iceberg this includes all
  // columns since Iceberg writes partition values into data files.
  const std::vector<column_index_t> dataChannels_;
  // Data-column indices used to sort bucketed output, empty when the table
  // has no bucket sort specification.
  const std::vector<column_index_t> sortColumnIndices_;
  // Compare flags (ascending/descending, nulls-first/last) aligned with
  // sortColumnIndices_.
  const std::vector<CompareFlags> sortCompareFlags_;
};

} // namespace facebook::velox::connector::hive
