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
#include "velox/connectors/hive/LogicalWriterFactory.h"

#include "velox/common/base/Counters.h"
#include "velox/common/base/Fs.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveDataSink.h"
#include "velox/connectors/hive/RotationWriter.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/Options.h"
#include "velox/dwio/common/SortingWriter.h"
#include "velox/dwio/dwrf/writer/Writer.h"
#include "velox/dwio/parquet/writer/Writer.h"
#include "velox/exec/SortBuffer.h"

namespace facebook::velox::connector::hive {
namespace {

constexpr const char* kEarlyFlushedRawBytes = "earlyFlushedRawBytes";

// Appends a sequence number to a filename for file rotation.
std::string makeSequencedFileName(
    const std::string& filename,
    uint32_t sequenceNumber) {
  if (sequenceNumber == 0) {
    return filename;
  }
  const auto dotPos = filename.rfind('.');
  if (dotPos == std::string::npos) {
    return fmt::format("{}_{}", filename, sequenceNumber);
  }
  return fmt::format(
      "{}_{}{}",
      filename.substr(0, dotPos),
      sequenceNumber,
      filename.substr(dotPos));
}

std::unique_ptr<dwio::common::FileSink> createHiveFileSink(
    const std::string& path,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    memory::MemoryPool* sinkPool,
    io::IoStatistics* ioStats,
    IoStats* fileSystemStats) {
  return dwio::common::FileSink::create(
      path,
      {
          .bufferWrite = false,
          .connectorProperties = hiveConfig->config(),
          .fileCreateConfig = hiveConfig->writeFileCreateConfig(),
          .pool = sinkPool,
          .metricLogger = dwio::common::MetricsLog::voidLog(),
          .stats = ioStats,
          .fileSystemStats = fileSystemStats,
      });
}

std::string makePartitionDirectory(
    const std::string& tableDirectory,
    const std::optional<std::string>& partitionSubdirectory) {
  if (partitionSubdirectory.has_value()) {
    return (fs::path(tableDirectory) / partitionSubdirectory.value()).string();
  }
  return tableDirectory;
}

std::shared_ptr<memory::MemoryPool> createSinkPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sink", writerPool->name()));
}

std::shared_ptr<memory::MemoryPool> createSortPool(
    const std::shared_ptr<memory::MemoryPool>& writerPool) {
  return writerPool->addLeafChild(fmt::format("{}.sort", writerPool->name()));
}

uint64_t getFinishTimeSliceLimitMs(
    const std::shared_ptr<const HiveConfig>& config,
    const config::ConfigBase* sessions) {
  const auto configuredLimit =
      config->sortWriterFinishTimeSliceLimitMs(sessions);
  return configuredLimit == 0 ? std::numeric_limits<uint64_t>::max()
                              : configuredLimit;
}

bool canReclaimWriters(
    const HiveInsertTableHandle& insertTableHandle,
    const ConnectorQueryCtx& connectorQueryCtx) {
  if (connectorQueryCtx.spillConfig() == nullptr) {
    return false;
  }
  return insertTableHandle.storageFormat() == dwio::common::FileFormat::DWRF ||
      insertTableHandle.storageFormat() == dwio::common::FileFormat::NIMBLE;
}

std::shared_ptr<dwio::common::WriterOptions> cloneWriterOptions(
    const std::shared_ptr<dwio::common::WriterOptions>& options,
    dwio::common::FileFormat format) {
  VELOX_CHECK_NOT_NULL(options);
  switch (format) {
    case dwio::common::FileFormat::DWRF:
    case dwio::common::FileFormat::NIMBLE: {
      auto typedOptions =
          std::dynamic_pointer_cast<dwrf::WriterOptions>(options);
      VELOX_CHECK_NOT_NULL(typedOptions);
      return std::make_shared<dwrf::WriterOptions>(*typedOptions);
    }
    case dwio::common::FileFormat::PARQUET: {
      auto typedOptions =
          std::dynamic_pointer_cast<parquet::WriterOptions>(options);
      VELOX_CHECK_NOT_NULL(typedOptions);
      return std::make_shared<parquet::WriterOptions>(*typedOptions);
    }
    default:
      VELOX_UNSUPPORTED(
          "Unsupported writer options cloning for file format {}",
          dwio::common::toString(format));
  }
}

std::vector<column_index_t> createSortColumnIndices(
    const HiveInsertTableHandle& insertTableHandle,
    const std::vector<column_index_t>& dataChannels,
    const RowTypePtr& inputType) {
  std::vector<column_index_t> sortColumnIndices;
  const auto* bucketProperty = insertTableHandle.bucketProperty();
  if (bucketProperty == nullptr) {
    return sortColumnIndices;
  }
  const auto& sortedProperty = bucketProperty->sortedBy();
  if (sortedProperty.empty()) {
    return sortColumnIndices;
  }

  auto dataType = getNonPartitionTypes(dataChannels, inputType);
  sortColumnIndices.reserve(sortedProperty.size());
  for (const auto& sortedColumn : sortedProperty) {
    auto columnIndex =
        dataType->getChildIdxIfExists(sortedColumn->sortColumn());
    if (columnIndex.has_value()) {
      sortColumnIndices.push_back(columnIndex.value());
    }
  }
  return sortColumnIndices;
}

std::vector<CompareFlags> createSortCompareFlags(
    const HiveInsertTableHandle& insertTableHandle) {
  std::vector<CompareFlags> sortCompareFlags;
  const auto* bucketProperty = insertTableHandle.bucketProperty();
  if (bucketProperty == nullptr) {
    return sortCompareFlags;
  }
  const auto& sortedProperty = bucketProperty->sortedBy();
  sortCompareFlags.reserve(sortedProperty.size());
  for (const auto& sortedColumn : sortedProperty) {
    sortCompareFlags.push_back(
        {sortedColumn->sortOrder().isNullsFirst(),
         sortedColumn->sortOrder().isAscending(),
         false,
         CompareFlags::NullHandlingMode::kNullAsValue});
  }
  return sortCompareFlags;
}

class WriterReclaimer : public exec::MemoryReclaimer {
 public:
  /// Creates a memory reclaimer bound to one logical writer.
  static std::unique_ptr<memory::MemoryReclaimer>
  create(bool canReclaim, WriterInfo* writerInfo, io::IoStatistics* ioStats) {
    return std::unique_ptr<memory::MemoryReclaimer>(
        new WriterReclaimer(canReclaim, writerInfo, ioStats));
  }

  /// Reports how many bytes can be reclaimed from the writer's pool.
  bool reclaimableBytes(
      const memory::MemoryPool& pool,
      uint64_t& reclaimableBytes) const override {
    VELOX_CHECK_EQ(pool.name(), writerInfo_->writerPool->name());
    reclaimableBytes = 0;
    if (!canReclaim_) {
      return false;
    }
    return exec::MemoryReclaimer::reclaimableBytes(pool, reclaimableBytes);
  }

  /// Triggers writer-level reclamation and reports the reclaimed bytes.
  uint64_t reclaim(
      memory::MemoryPool* pool,
      uint64_t targetBytes,
      uint64_t maxWaitMs,
      memory::MemoryReclaimer::Stats& stats) override {
    VELOX_CHECK_EQ(pool->name(), writerInfo_->writerPool->name());
    if (!canReclaim_) {
      return 0;
    }

    if (*writerInfo_->nonReclaimableSectionHolder.get()) {
      RECORD_METRIC_VALUE(kMetricMemoryNonReclaimableCount);
      LOG(WARNING) << "Can't reclaim from hive writer pool " << pool->name()
                   << " which is under non-reclaimable section, root pool: "
                   << pool->root()->name()
                   << ", reservation: " << succinctBytes(pool->reservedBytes());
      ++stats.numNonReclaimableAttempts;
      return 0;
    }

    const auto writtenBytesBeforeReclaim = ioStats_->rawBytesWritten();
    const auto reclaimedBytes =
        exec::MemoryReclaimer::reclaim(pool, targetBytes, maxWaitMs, stats);
    const auto earlyFlushedRawBytes =
        ioStats_->rawBytesWritten() - writtenBytesBeforeReclaim;
    addThreadLocalRuntimeStat(
        kEarlyFlushedRawBytes,
        RuntimeCounter(earlyFlushedRawBytes, RuntimeCounter::Unit::kBytes));
    if (earlyFlushedRawBytes > 0) {
      RECORD_METRIC_VALUE(
          kMetricFileWriterEarlyFlushedRawBytes, earlyFlushedRawBytes);
    }
    return reclaimedBytes;
  }

 private:
  WriterReclaimer(
      bool canReclaim,
      WriterInfo* writerInfo,
      io::IoStatistics* ioStats)
      : exec::MemoryReclaimer(0),
        canReclaim_(canReclaim),
        writerInfo_(writerInfo),
        ioStats_(ioStats) {
    VELOX_CHECK_NOT_NULL(writerInfo_);
    VELOX_CHECK_NOT_NULL(ioStats_);
  }

  // Indicates whether the associated writer supports memory reclaim.
  const bool canReclaim_;
  // References the writer state whose pool is being reclaimed.
  WriterInfo* const writerInfo_;
  // Records bytes flushed during reclaim so runtime stats stay accurate.
  io::IoStatistics* const ioStats_;
};

} // namespace

RowTypePtr getNonPartitionTypes(
    const std::vector<column_index_t>& dataChannels,
    const RowTypePtr& inputType) {
  std::vector<std::string> childNames;
  std::vector<TypePtr> childTypes;
  childNames.reserve(dataChannels.size());
  childTypes.reserve(dataChannels.size());
  for (auto channel : dataChannels) {
    childNames.push_back(inputType->nameOf(channel));
    childTypes.push_back(inputType->childAt(channel));
  }
  return ROW(std::move(childNames), std::move(childTypes));
}

LogicalWriterFactory::LogicalWriterFactory(
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
    IoStats* fileSystemStats)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      hiveConfig_(std::move(hiveConfig)),
      updateMode_(updateMode),
      commitRequired_(commitRequired),
      partitionNameFactory_(std::move(partitionNameFactory)),
      writerOptionsCustomizer_(std::move(writerOptionsCustomizer)),
      fileClosedCallback_(std::move(fileClosedCallback)),
      fileSystemStats_(fileSystemStats),
      leafWriterFactory_(
          dwio::common::getWriterFactory(insertTableHandle_->storageFormat())),
      spillConfig_(connectorQueryCtx_->spillConfig()),
      sortWriterFinishTimeSliceLimitMs_(getFinishTimeSliceLimitMs(
          hiveConfig_,
          connectorQueryCtx_->sessionProperties())),
      maxTargetFileBytes_(hiveConfig_->maxTargetFileSizeBytes(
          connectorQueryCtx_->sessionProperties())),
      canReclaim_(canReclaimWriters(*insertTableHandle_, *connectorQueryCtx_)),
      dataChannels_(dataChannels),
      sortColumnIndices_(createSortColumnIndices(
          *insertTableHandle_,
          dataChannels_,
          inputType_)),
      sortCompareFlags_(createSortCompareFlags(*insertTableHandle_)) {
  VELOX_CHECK_NOT_NULL(connectorQueryCtx_);
  VELOX_CHECK_NOT_NULL(fileSystemStats_);
  VELOX_CHECK_NOT_NULL(leafWriterFactory_);
}

std::unique_ptr<LogicalWriter> LogicalWriterFactory::createWriter(
    const WriterId& id,
    uint32_t writerIndex) {
  auto writerParameters = getWriterParameters(id);
  auto writerPool = createWriterPool(id);
  auto sinkPool = createSinkPool(writerPool);
  std::shared_ptr<memory::MemoryPool> sortPool{nullptr};
  if (!sortColumnIndices_.empty()) {
    sortPool = createSortPool(writerPool);
  }
  auto writerInfo = std::make_shared<WriterInfo>(
      std::move(writerParameters),
      std::move(writerPool),
      std::move(sinkPool),
      std::move(sortPool));
  auto ioStats = std::make_unique<io::IoStatistics>();

  setMemoryReclaimers(writerInfo.get(), ioStats.get());
  auto writer = createFormatWriter(writerInfo.get(), ioStats.get());
  addThreadLocalRuntimeStat(
      fmt::format(
          "{}WriterCount",
          dwio::common::toString(insertTableHandle_->storageFormat())),
      RuntimeCounter(1));

  auto* ioStatsRaw = ioStats.get();
  auto writerInfoPtr = writerInfo;
  return std::make_unique<RotationWriter>(
      std::move(writer),
      std::move(writerInfo),
      std::move(ioStats),
      maxTargetFileBytes_,
      id.bucketId == std::nullopt && sortColumnIndices_.empty(),
      [this, writerInfoPtr, ioStatsRaw]() {
        return createFormatWriter(writerInfoPtr.get(), ioStatsRaw);
      },
      [this, writerIndex](
          std::optional<FileInfo> fileInfo,
          std::unique_ptr<dwio::common::FileMetadata> metadata) {
        if (fileClosedCallback_) {
          fileClosedCallback_(
              writerIndex, std::move(fileInfo), std::move(metadata));
        }
      });
}

std::shared_ptr<memory::MemoryPool> LogicalWriterFactory::createWriterPool(
    const WriterId& writerId) const {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  return connectorPool->addAggregateChild(
      fmt::format("{}.{}", connectorPool->name(), writerId.toString()));
}

void LogicalWriterFactory::setMemoryReclaimers(
    WriterInfo* writerInfo,
    io::IoStatistics* ioStats) const {
  auto* connectorPool = connectorQueryCtx_->connectorMemoryPool();
  if (connectorPool->reclaimer() == nullptr) {
    return;
  }
  writerInfo->writerPool->setReclaimer(
      WriterReclaimer::create(canReclaim_, writerInfo, ioStats));
  writerInfo->sinkPool->setReclaimer(exec::MemoryReclaimer::create());
}

std::unique_ptr<dwio::common::Writer> LogicalWriterFactory::createFormatWriter(
    WriterInfo* writerInfo,
    io::IoStatistics* ioStats) const {
  VELOX_CHECK_NOT_NULL(writerInfo);
  VELOX_CHECK_NOT_NULL(ioStats);

  const auto& params = writerInfo->writerParameters;
  writerInfo->currentWriteFileName = makeSequencedFileName(
      params.writeFileName(), writerInfo->fileSequenceNumber);
  writerInfo->currentTargetFileName = makeSequencedFileName(
      params.targetFileName(), writerInfo->fileSequenceNumber);

  const auto writePath =
      (fs::path(params.writeDirectory()) / writerInfo->currentWriteFileName)
          .string();

  auto options = createWriterOptions(*writerInfo);
  memory::NonReclaimableSectionGuard nonReclaimableGuard(
      writerInfo->nonReclaimableSectionHolder.get());
  auto writer = leafWriterFactory_->createWriter(
      createHiveFileSink(
          writePath,
          hiveConfig_,
          writerInfo->sinkPool.get(),
          ioStats,
          fileSystemStats_),
      options);
  return maybeCreateSortingWriter(writerInfo, std::move(writer));
}

std::shared_ptr<dwio::common::WriterOptions>
LogicalWriterFactory::createWriterOptions(const WriterInfo& writerInfo) const {
  std::shared_ptr<dwio::common::WriterOptions> options;
  if (auto baseOptions = insertTableHandle_->writerOptions()) {
    options =
        cloneWriterOptions(baseOptions, insertTableHandle_->storageFormat());
  } else {
    options = std::shared_ptr<dwio::common::WriterOptions>(
        leafWriterFactory_->createWriterOptions().release());
  }

  if (options->schema == nullptr) {
    options->schema = getNonPartitionTypes(dataChannels_, inputType_);
  }
  if (options->memoryPool == nullptr) {
    options->memoryPool = writerInfo.writerPool.get();
  }
  if (!options->compressionKind) {
    options->compressionKind = insertTableHandle_->compressionKind();
  }
  if (options->spillConfig == nullptr && canReclaim_) {
    options->spillConfig = spillConfig_;
  }
  options->nonReclaimableSection = writerInfo.nonReclaimableSectionHolder.get();
  if (options->memoryReclaimerFactory == nullptr ||
      options->memoryReclaimerFactory() == nullptr) {
    options->memoryReclaimerFactory = []() {
      return exec::MemoryReclaimer::create();
    };
  }
  if (options->serdeParameters.empty()) {
    options->serdeParameters = std::map<std::string, std::string>(
        insertTableHandle_->serdeParameters().begin(),
        insertTableHandle_->serdeParameters().end());
  }

  options->sessionTimezoneName = connectorQueryCtx_->sessionTimezone();
  options->adjustTimestampToTimezone =
      connectorQueryCtx_->adjustTimestampToTimezone();
  options->processConfigs(
      *hiveConfig_->config(), *connectorQueryCtx_->sessionProperties());
  if (writerOptionsCustomizer_) {
    writerOptionsCustomizer_(writerInfo, options);
  }
  return options;
}

WriterParameters LogicalWriterFactory::getWriterParameters(
    const WriterId& id) const {
  auto [targetFileName, writeFileName] = getWriterFileNames(id.bucketId);
  const auto partitionName =
      partitionNameFactory_ ? partitionNameFactory_(id) : std::nullopt;
  return WriterParameters{
      updateMode_,
      partitionName,
      targetFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->targetPath(), partitionName),
      writeFileName,
      makePartitionDirectory(
          insertTableHandle_->locationHandle()->writePath(), partitionName)};
}

std::pair<std::string, std::string> LogicalWriterFactory::getWriterFileNames(
    std::optional<uint32_t> bucketId) const {
  if (auto hiveInsertFileNameGenerator =
          std::dynamic_pointer_cast<const HiveInsertFileNameGenerator>(
              insertTableHandle_->fileNameGenerator())) {
    return hiveInsertFileNameGenerator->gen(
        bucketId,
        insertTableHandle_,
        *connectorQueryCtx_,
        hiveConfig_,
        commitRequired_);
  }

  return insertTableHandle_->fileNameGenerator()->gen(
      bucketId, insertTableHandle_, *connectorQueryCtx_, commitRequired_);
}

std::unique_ptr<dwio::common::Writer>
LogicalWriterFactory::maybeCreateSortingWriter(
    WriterInfo* writerInfo,
    std::unique_ptr<dwio::common::Writer> writer) const {
  if (sortColumnIndices_.empty()) {
    return writer;
  }
  VELOX_CHECK_NOT_NULL(writerInfo);
  auto* sortPool = writerInfo->sortPool.get();
  VELOX_CHECK_NOT_NULL(sortPool);
  auto sortBuffer = std::make_unique<exec::SortBuffer>(
      getNonPartitionTypes(dataChannels_, inputType_),
      sortColumnIndices_,
      sortCompareFlags_,
      sortPool,
      writerInfo->nonReclaimableSectionHolder.get(),
      connectorQueryCtx_->prefixSortConfig(),
      spillConfig_,
      writerInfo->spillStats.get());

  return std::make_unique<dwio::common::SortingWriter>(
      std::move(writer),
      std::move(sortBuffer),
      hiveConfig_->sortWriterMaxOutputRows(
          connectorQueryCtx_->sessionProperties()),
      hiveConfig_->sortWriterMaxOutputBytes(
          connectorQueryCtx_->sessionProperties()),
      sortWriterFinishTimeSliceLimitMs_);
}

} // namespace facebook::velox::connector::hive
