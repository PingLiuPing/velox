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

#include "velox/connectors/hive/LogicalWriter.h"
#include "velox/connectors/hive/WriterInfo.h"

namespace facebook::velox::connector::hive {

/// Decorates a format writer with file-rotation semantics and per-writer
/// bookkeeping. When rotation is enabled (unbucketed, unsorted writers), the
/// wrapper automatically closes the current file and opens a new one once the
/// file size exceeds a configured threshold. When rotation is disabled
/// (bucketed or sorted writers), all data is written to a single file. In both
/// modes the wrapper tracks file-level metadata, lazily recreates the inner
/// writer after rotation, and exposes lifecycle callbacks for format-specific
/// post-processing such as Iceberg statistics collection.
class RotationWriter : public LogicalWriter {
 public:
  /// Creates the next inner writer instance after a rotation.
  using WriterCreator = std::function<std::unique_ptr<dwio::common::Writer>()>;
  /// Observes completed files after they have been finalized and recorded in
  /// the shared writer state.
  using FileClosedCallback = std::function<void(
      std::optional<FileInfo>,
      std::unique_ptr<dwio::common::FileMetadata>)>;

  /// Creates a rotation-aware logical writer around an initial inner writer.
  RotationWriter(
      std::unique_ptr<dwio::common::Writer> writer,
      std::shared_ptr<WriterInfo> writerInfo,
      std::unique_ptr<io::IoStatistics> ioStats,
      uint64_t maxTargetFileBytes,
      bool canRotate,
      WriterCreator writerCreator,
      FileClosedCallback fileClosedCallback = nullptr);

  /// Appends data to the current file and rotates when the size threshold is
  /// exceeded.
  void write(const VectorPtr& data) override;
  /// Flushes the current inner writer when one is active.
  void flush() override;
  /// Finishes buffered writer state and propagates yielding from inner
  /// decorators such as SortingWriter.
  bool finish() override;
  /// Closes the current file, finalizes its metadata, and invokes the
  /// file-closed callback.
  std::unique_ptr<dwio::common::FileMetadata> close() override;
  /// Aborts the current inner writer and drops any uncommitted output.
  void abort() override;

  /// Returns the shared execution state for this logical writer.
  const std::shared_ptr<WriterInfo>& writerInfo() const override {
    return writerInfo_;
  }

  /// Returns the IO statistics accumulated across all files produced by this
  /// logical writer.
  io::IoStatistics* ioStats() const override {
    return ioStats_.get();
  }

 private:
  // Lazily recreates the inner writer after rotation.
  void ensureWriter();
  // Finalizes the current file and records its metadata in writerInfo_.
  std::optional<FileInfo> finalizeWriterFile();
  // Closes the active inner writer, publishes the completed file callback,
  // and optionally releases the closed writer instance.
  std::unique_ptr<dwio::common::FileMetadata> closeWriter(bool releaseWriter);
  // Rotates to a fresh inner writer on the next write call.
  void rotateWriter();
  // Returns the number of bytes written into the currently active file.
  uint64_t getCurrentFileBytes() const;

  // Shares mutable execution state with the sink and commit protocol.
  std::shared_ptr<WriterInfo> writerInfo_;
  // Accumulates IO statistics across every rotated file.
  std::unique_ptr<io::IoStatistics> ioStats_;
  // Owns the currently active inner writer, if one is open.
  std::unique_ptr<dwio::common::Writer> writer_;
  // Stores the file-size threshold that triggers rotation.
  uint64_t maxTargetFileBytes_;
  // Controls whether rotation is enabled for this logical writer.
  bool canRotate_;
  // Recreates the inner writer after a rotation.
  WriterCreator writerCreator_;
  // Observes completed files after they have been finalized.
  FileClosedCallback fileClosedCallback_;
};

} // namespace facebook::velox::connector::hive
