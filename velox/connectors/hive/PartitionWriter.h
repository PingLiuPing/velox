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
#include <vector>

#include <folly/container/F14Map.h>

#include "velox/common/memory/RawVector.h"
#include "velox/connectors/hive/LogicalWriter.h"
#include "velox/connectors/hive/WriterInfo.h"
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::hive {

class LogicalWriterFactory;

/// Routes input rows to logical writers keyed by partition and bucket ids.
/// Implementations decide how many writers remain open at once and how the
/// input batch is partitioned before dispatch.
class PartitionWriter {
 public:
  /// Observes newly created writers so higher layers can attach side effects.
  using WriterCreatedCallback = std::function<void(const WriterId&, uint32_t)>;

  /// Destroys the partition writer strategy and all owned logical writers.
  virtual ~PartitionWriter() = default;

  /// Ensures that a logical writer exists for the provided identifier and
  /// returns its stable index.
  virtual uint32_t ensureWriter(const WriterId& id) = 0;
  /// Writes a batch that belongs entirely to one writer id.
  virtual void write(const WriterId& id, const RowVectorPtr& input) = 0;
  /// Writes a mixed batch by routing each row to the writer identified by the
  /// supplied partition and bucket vectors.
  virtual void write(
      const RowVectorPtr& input,
      const raw_vector<uint64_t>& partitionIds,
      const std::vector<uint32_t>& bucketIds,
      bool isPartitioned,
      bool isBucketed) = 0;
  /// Finishes any buffered writer state and returns false when a writer yields
  /// and needs another finish() call.
  virtual bool finish() = 0;
  /// Closes all logical writers and finalizes their files.
  virtual void close() = 0;
  /// Aborts all logical writers and drops any uncommitted output.
  virtual void abort() = 0;
  /// Returns the owned logical writers in their stable writer-index order.
  virtual const std::vector<std::unique_ptr<LogicalWriter>>& writers()
      const = 0;
};

/// Fan-out implementation that keeps one logical writer per active
/// partition/bucket open at the same time and dispatches rows by materializing
/// per-writer row index lists for each input batch.
class FanoutPartitionWriter final : public PartitionWriter {
 public:
  /// Creates a fan-out writer that can keep up to maxOpenWriters logical
  /// writers active concurrently.
  FanoutPartitionWriter(
      uint32_t maxOpenWriters,
      LogicalWriterFactory* writerFactory,
      WriterCreatedCallback writerCreatedCallback,
      memory::MemoryPool* pool);

  /// Ensures a writer exists for the provided partition/bucket identifier.
  uint32_t ensureWriter(const WriterId& id) override;
  /// Writes a batch that already belongs to a single writer.
  void write(const WriterId& id, const RowVectorPtr& input) override;
  /// Splits a mixed batch into per-writer row subsets and dispatches them.
  void write(
      const RowVectorPtr& input,
      const raw_vector<uint64_t>& partitionIds,
      const std::vector<uint32_t>& bucketIds,
      bool isPartitioned,
      bool isBucketed) override;
  /// Finishes all owned logical writers and propagates yielding.
  bool finish() override;
  /// Closes every owned logical writer.
  void close() override;
  /// Aborts every owned logical writer.
  void abort() override;

  /// Returns the owned logical writers in writer-index order.
  const std::vector<std::unique_ptr<LogicalWriter>>& writers() const override {
    return writers_;
  }

 private:
  // Computes the writer identifier for a single input row.
  WriterId getWriterId(
      size_t row,
      const raw_vector<uint64_t>& partitionIds,
      const std::vector<uint32_t>& bucketIds,
      bool isPartitioned,
      bool isBucketed) const;
  // Appends a new logical writer and returns its stable writer index.
  uint32_t appendWriter(const WriterId& id);
  // Records one input row index for a specific writer in the current batch.
  void
  updatePartitionRows(uint32_t index, vector_size_t numRows, vector_size_t row);
  // Builds the per-writer row-index lists for the current mixed input batch.
  void splitInputRowsAndEnsureWriters(
      const raw_vector<uint64_t>& partitionIds,
      const std::vector<uint32_t>& bucketIds,
      bool isPartitioned,
      bool isBucketed);

  // Caps how many logical writers may remain active at once.
  const uint32_t maxOpenWriters_;
  // Creates logical writers on demand.
  LogicalWriterFactory* const writerFactory_;
  // Observes newly created writers for format-specific side effects.
  const WriterCreatedCallback writerCreatedCallback_;
  // Allocates the per-batch row-index buffers used for fan-out routing.
  memory::MemoryPool* const pool_;

  // Maps a writer id to its stable index in writers_.
  folly::F14FastMap<WriterId, uint32_t, WriterIdHasher, WriterIdEq>
      writerIndexMap_;
  // Owns the active logical writers in stable writer-index order.
  std::vector<std::unique_ptr<LogicalWriter>> writers_;
  // Owns the row-index buffers reused across input batches.
  std::vector<BufferPtr> partitionRows_;
  // Caches raw pointers into partitionRows_ for faster writes.
  std::vector<vector_size_t*> rawPartitionRows_;
  // Stores the number of rows assigned to each writer in the current batch.
  std::vector<vector_size_t> partitionSizes_;
};

} // namespace facebook::velox::connector::hive
