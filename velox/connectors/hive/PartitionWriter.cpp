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

#include "velox/connectors/hive/PartitionWriter.h"

#include "velox/connectors/hive/LogicalWriterFactory.h"
#include "velox/exec/OperatorUtils.h"

namespace facebook::velox::connector::hive {

FanoutPartitionWriter::FanoutPartitionWriter(
    uint32_t maxOpenWriters,
    LogicalWriterFactory* writerFactory,
    WriterCreatedCallback writerCreatedCallback,
    memory::MemoryPool* pool)
    : maxOpenWriters_(maxOpenWriters),
      writerFactory_(writerFactory),
      writerCreatedCallback_(std::move(writerCreatedCallback)),
      pool_(pool) {
  VELOX_CHECK_NOT_NULL(writerFactory_);
  VELOX_CHECK_NOT_NULL(pool_);
}

uint32_t FanoutPartitionWriter::ensureWriter(const WriterId& id) {
  auto it = writerIndexMap_.find(id);
  if (it != writerIndexMap_.end()) {
    return it->second;
  }
  return appendWriter(id);
}

void FanoutPartitionWriter::write(
    const WriterId& id,
    const RowVectorPtr& input) {
  const auto index = ensureWriter(id);
  writers_[index]->write(input);
}

void FanoutPartitionWriter::write(
    const RowVectorPtr& input,
    const raw_vector<uint64_t>& partitionIds,
    const std::vector<uint32_t>& bucketIds,
    bool isPartitioned,
    bool isBucketed) {
  splitInputRowsAndEnsureWriters(
      partitionIds, bucketIds, isPartitioned, isBucketed);

  for (auto index = 0; index < writers_.size(); ++index) {
    const vector_size_t partitionSize = partitionSizes_[index];
    if (partitionSize == 0) {
      continue;
    }

    RowVectorPtr writerInput = partitionSize == input->size()
        ? input
        : exec::wrap(partitionSize, partitionRows_[index], input);
    writers_[index]->write(writerInput);
  }
}

bool FanoutPartitionWriter::finish() {
  for (const auto& writer : writers_) {
    if (!writer->finish()) {
      return false;
    }
  }
  return true;
}

void FanoutPartitionWriter::close() {
  for (const auto& writer : writers_) {
    writer->close();
  }
}

void FanoutPartitionWriter::abort() {
  for (const auto& writer : writers_) {
    writer->abort();
  }
}

WriterId FanoutPartitionWriter::getWriterId(
    size_t row,
    const raw_vector<uint64_t>& partitionIds,
    const std::vector<uint32_t>& bucketIds,
    bool isPartitioned,
    bool isBucketed) const {
  std::optional<uint32_t> partitionId;
  if (isPartitioned) {
    VELOX_CHECK_LT(partitionIds[row], std::numeric_limits<uint32_t>::max());
    partitionId = static_cast<uint32_t>(partitionIds[row]);
  }

  std::optional<uint32_t> bucketId;
  if (isBucketed) {
    bucketId = bucketIds[row];
  }

  return WriterId{partitionId, bucketId};
}

uint32_t FanoutPartitionWriter::appendWriter(const WriterId& id) {
  VELOX_USER_CHECK_LT(
      writers_.size(), maxOpenWriters_, "Exceeded open writer limit");
  writers_.push_back(writerFactory_->createWriter(id, writers_.size()));
  partitionSizes_.emplace_back(0);
  partitionRows_.emplace_back(nullptr);
  rawPartitionRows_.emplace_back(nullptr);
  writerIndexMap_.emplace(id, writers_.size() - 1);

  if (writerCreatedCallback_) {
    writerCreatedCallback_(id, writers_.size() - 1);
  }
  return writers_.size() - 1;
}

void FanoutPartitionWriter::updatePartitionRows(
    uint32_t index,
    vector_size_t numRows,
    vector_size_t row) {
  VELOX_DCHECK_LT(index, partitionSizes_.size());
  VELOX_DCHECK_EQ(partitionSizes_.size(), partitionRows_.size());
  VELOX_DCHECK_EQ(partitionRows_.size(), rawPartitionRows_.size());
  if (FOLLY_UNLIKELY(partitionRows_[index] == nullptr) ||
      (partitionRows_[index]->capacity() < numRows * sizeof(vector_size_t))) {
    partitionRows_[index] = allocateIndices(numRows, pool_);
    rawPartitionRows_[index] =
        partitionRows_[index]->asMutable<vector_size_t>();
  }
  rawPartitionRows_[index][partitionSizes_[index]] = row;
  ++partitionSizes_[index];
}

void FanoutPartitionWriter::splitInputRowsAndEnsureWriters(
    const raw_vector<uint64_t>& partitionIds,
    const std::vector<uint32_t>& bucketIds,
    bool isPartitioned,
    bool isBucketed) {
  VELOX_CHECK(isPartitioned || isBucketed);
  if (isPartitioned && isBucketed) {
    VELOX_CHECK_EQ(partitionIds.size(), bucketIds.size());
  }

  std::fill(partitionSizes_.begin(), partitionSizes_.end(), 0);

  const auto numRows = isPartitioned ? partitionIds.size() : bucketIds.size();
  for (auto row = 0; row < numRows; ++row) {
    const auto id =
        getWriterId(row, partitionIds, bucketIds, isPartitioned, isBucketed);
    const auto index = ensureWriter(id);
    updatePartitionRows(index, numRows, row);
  }

  for (uint32_t i = 0; i < partitionSizes_.size(); ++i) {
    if (partitionSizes_[i] != 0) {
      VELOX_CHECK_NOT_NULL(partitionRows_[i]);
      partitionRows_[i]->setSize(partitionSizes_[i] * sizeof(vector_size_t));
    }
  }
}

} // namespace facebook::velox::connector::hive
