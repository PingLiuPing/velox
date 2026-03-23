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

#include "velox/connectors/hive/RotationWriter.h"

#include "velox/common/memory/MemoryArbitrator.h"

namespace facebook::velox::connector::hive {

RotationWriter::RotationWriter(
    std::unique_ptr<dwio::common::Writer> writer,
    std::shared_ptr<WriterInfo> writerInfo,
    std::unique_ptr<io::IoStatistics> ioStats,
    uint64_t maxTargetFileBytes,
    bool canRotate,
    WriterCreator writerCreator,
    FileClosedCallback fileClosedCallback)
    : writerInfo_(std::move(writerInfo)),
      ioStats_(std::move(ioStats)),
      writer_(std::move(writer)),
      maxTargetFileBytes_(maxTargetFileBytes),
      canRotate_(canRotate),
      writerCreator_(std::move(writerCreator)),
      fileClosedCallback_(std::move(fileClosedCallback)) {
  VELOX_CHECK_NOT_NULL(writerInfo_);
  VELOX_CHECK_NOT_NULL(ioStats_);
  VELOX_CHECK_NOT_NULL(writerCreator_);
  setState(State::kRunning);
}

void RotationWriter::write(const VectorPtr& data) {
  checkRunning();

  memory::NonReclaimableSectionGuard nonReclaimableGuard(
      writerInfo_->nonReclaimableSectionHolder.get());
  ensureWriter();
  writer_->write(data);
  writerInfo_->inputSizeInBytes += data->estimateFlatSize();
  writerInfo_->numWrittenRows += data->size();
  writerInfo_->currentFileWrittenRows += data->size();

  if (maxTargetFileBytes_ == 0 || !canRotate_) {
    return;
  }

  if (getCurrentFileBytes() >= maxTargetFileBytes_) {
    rotateWriter();
  }
}

void RotationWriter::flush() {
  checkRunning();
  if (writer_ != nullptr) {
    memory::NonReclaimableSectionGuard nonReclaimableGuard(
        writerInfo_->nonReclaimableSectionHolder.get());
    writer_->flush();
  }
}

bool RotationWriter::finish() {
  setState(State::kFinishing);
  if (writer_ == nullptr) {
    return true;
  }

  memory::NonReclaimableSectionGuard nonReclaimableGuard(
      writerInfo_->nonReclaimableSectionHolder.get());
  return writer_->finish();
}

std::unique_ptr<dwio::common::FileMetadata> RotationWriter::close() {
  if (isRunning()) {
    setState(State::kFinishing);
  }
  setState(State::kClosed);
  if (writer_ == nullptr) {
    return nullptr;
  }

  memory::NonReclaimableSectionGuard nonReclaimableGuard(
      writerInfo_->nonReclaimableSectionHolder.get());
  return closeWriter(false);
}

void RotationWriter::abort() {
  setState(State::kAborted);
  if (writer_ == nullptr) {
    return;
  }

  memory::NonReclaimableSectionGuard nonReclaimableGuard(
      writerInfo_->nonReclaimableSectionHolder.get());
  writer_->abort();
}

void RotationWriter::ensureWriter() {
  if (writer_ == nullptr) {
    writer_ = writerCreator_();
  }
}

std::optional<FileInfo> RotationWriter::finalizeWriterFile() {
  const auto currentFileBytes = getCurrentFileBytes();
  std::optional<FileInfo> fileInfo;
  if (currentFileBytes > 0) {
    fileInfo = FileInfo{
        .writeFileName = writerInfo_->currentWriteFileName,
        .targetFileName = writerInfo_->currentTargetFileName,
        .fileSize = currentFileBytes,
        .numRows = writerInfo_->currentFileWrittenRows,
    };
    writerInfo_->currentFileWrittenRows = 0;
    writerInfo_->writtenFiles.push_back(*fileInfo);
  }

  writerInfo_->cumulativeWrittenBytes = ioStats_->rawBytesWritten();
  return fileInfo;
}

std::unique_ptr<dwio::common::FileMetadata> RotationWriter::closeWriter(
    bool releaseWriter) {
  auto metadata = writer_->close();
  auto fileInfo = finalizeWriterFile();
  if (releaseWriter) {
    writer_.reset();
  }
  if (fileClosedCallback_) {
    fileClosedCallback_(std::move(fileInfo), std::move(metadata));
    return nullptr;
  }
  return metadata;
}

void RotationWriter::rotateWriter() {
  closeWriter(true);
  ++writerInfo_->fileSequenceNumber;
}

uint64_t RotationWriter::getCurrentFileBytes() const {
  const auto totalBytes = ioStats_->rawBytesWritten();
  const auto baselineBytes = writerInfo_->cumulativeWrittenBytes;
  VELOX_DCHECK_GE(totalBytes, baselineBytes);
  return totalBytes - baselineBytes;
}

} // namespace facebook::velox::connector::hive
