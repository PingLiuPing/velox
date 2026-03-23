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

#include "velox/connectors/hive/HiveDataSink.h"

#include "velox/common/base/Fs.h"
#include "velox/common/base/StatsReporter.h"
#include "velox/common/testutil/TestValue.h"
#include "velox/connectors/hive/HiveCommitMessage.h"
#include "velox/connectors/hive/HiveConfig.h"
#include "velox/connectors/hive/HiveConnectorUtil.h"
#include "velox/connectors/hive/HivePartitionFunction.h"
#include "velox/connectors/hive/LogicalWriterFactory.h"
#include "velox/connectors/hive/PartitionWriter.h"
#include "velox/connectors/hive/TableHandle.h"
#include "velox/dwio/common/Options.h"
#include "velox/exec/OperatorUtils.h"

#include <boost/lexical_cast.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <re2/re2.h>

using facebook::velox::common::testutil::TestValue;

namespace facebook::velox::connector::hive {
namespace {

// Filters out partition columns if there is any.
RowVectorPtr makeDataInput(
    const std::vector<column_index_t>& dataCols,
    const RowVectorPtr& input) {
  std::vector<VectorPtr> childVectors;
  childVectors.reserve(dataCols.size());
  for (int dataCol : dataCols) {
    childVectors.push_back(input->childAt(dataCol));
  }

  return std::make_shared<RowVector>(
      input->pool(),
      getNonPartitionTypes(dataCols, asRowType(input->type())),
      input->nulls(),
      input->size(),
      std::move(childVectors),
      input->getNullCount());
}

// Creates a PartitionIdGenerator if the table is partitioned, otherwise returns
// nullptr.
std::unique_ptr<PartitionIdGenerator> createPartitionIdGenerator(
    const RowTypePtr& inputType,
    const std::shared_ptr<const HiveInsertTableHandle>& insertTableHandle,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    const ConnectorQueryCtx* connectorQueryCtx) {
  auto partitionChannels = insertTableHandle->partitionChannels();
  if (partitionChannels.empty()) {
    return nullptr;
  }
  return std::make_unique<PartitionIdGenerator>(
      inputType,
      partitionChannels,
      hiveConfig->maxPartitionsPerWriters(
          connectorQueryCtx->sessionProperties()),
      connectorQueryCtx->memoryPool());
}

std::string makeUuid() {
  return boost::lexical_cast<std::string>(boost::uuids::random_generator()());
}

std::unordered_map<LocationHandle::TableType, std::string> tableTypeNames() {
  return {
      {LocationHandle::TableType::kNew, "kNew"},
      {LocationHandle::TableType::kExisting, "kExisting"},
  };
}

template <typename K, typename V>
std::unordered_map<V, K> invertMap(const std::unordered_map<K, V>& mapping) {
  std::unordered_map<V, K> inverted;
  for (const auto& [key, value] : mapping) {
    inverted.emplace(value, key);
  }
  return inverted;
}

std::unique_ptr<core::PartitionFunction> createBucketFunction(
    const HiveBucketProperty& bucketProperty,
    const RowTypePtr& inputType) {
  const auto& bucketedBy = bucketProperty.bucketedBy();
  const auto& bucketedTypes = bucketProperty.bucketedTypes();
  std::vector<column_index_t> bucketedByChannels;
  bucketedByChannels.reserve(bucketedBy.size());
  for (int32_t i = 0; i < bucketedBy.size(); ++i) {
    const auto& bucketColumn = bucketedBy[i];
    const auto& bucketType = bucketedTypes[i];
    const auto inputChannel = inputType->getChildIdx(bucketColumn);
    if (FOLLY_UNLIKELY(
            !inputType->childAt(inputChannel)->equivalent(*bucketType))) {
      VELOX_USER_FAIL(
          "Input column {} type {} doesn't match bucket type {}",
          inputType->nameOf(inputChannel),
          inputType->childAt(inputChannel)->toString(),
          bucketType->toString());
    }
    bucketedByChannels.push_back(inputChannel);
  }
  return std::make_unique<HivePartitionFunction>(
      bucketProperty.bucketCount(), bucketedByChannels);
}

std::string computeBucketedFileName(
    const std::string& queryId,
    uint32_t maxBucketCount,
    uint32_t bucket) {
  const uint32_t kMaxBucketCountPadding =
      std::to_string(maxBucketCount - 1).size();
  const std::string bucketValueStr = std::to_string(bucket);
  return fmt::format(
      "0{:0>{}}_0_{}", bucketValueStr, kMaxBucketCountPadding, queryId);
}

FOLLY_ALWAYS_INLINE int32_t
getBucketCount(const HiveBucketProperty* bucketProperty) {
  return bucketProperty == nullptr ? 0 : bucketProperty->bucketCount();
}

std::vector<column_index_t> computePartitionChannels(
    const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns) {
  std::vector<column_index_t> channels;
  for (auto i = 0; i < inputColumns.size(); i++) {
    if (inputColumns[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }
  return channels;
}

std::vector<column_index_t> computeNonPartitionChannels(
    const std::vector<std::shared_ptr<const HiveColumnHandle>>& inputColumns) {
  std::vector<column_index_t> channels;
  for (auto i = 0; i < inputColumns.size(); i++) {
    if (!inputColumns[i]->isPartitionKey()) {
      channels.push_back(i);
    }
  }
  return channels;
}

} // namespace

const std::string LocationHandle::tableTypeName(
    LocationHandle::TableType type) {
  static const auto tableTypes = tableTypeNames();
  return tableTypes.at(type);
}

LocationHandle::TableType LocationHandle::tableTypeFromName(
    const std::string& name) {
  static const auto nameTableTypes = invertMap(tableTypeNames());
  return nameTableTypes.at(name);
}

HiveSortingColumn::HiveSortingColumn(
    const std::string& sortColumn,
    const core::SortOrder& sortOrder)
    : sortColumn_(sortColumn), sortOrder_(sortOrder) {
  VELOX_USER_CHECK(!sortColumn_.empty(), "hive sort column must be set");

  if (FOLLY_UNLIKELY(
          (sortOrder_.isAscending() && !sortOrder_.isNullsFirst()) ||
          (!sortOrder_.isAscending() && sortOrder_.isNullsFirst()))) {
    VELOX_USER_FAIL("Bad hive sort order: {}", toString());
  }
}

folly::dynamic HiveSortingColumn::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveSortingColumn";
  obj["columnName"] = sortColumn_;
  obj["sortOrder"] = sortOrder_.serialize();
  return obj;
}

std::shared_ptr<HiveSortingColumn> HiveSortingColumn::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const std::string columnName = obj["columnName"].asString();
  const auto sortOrder = core::SortOrder::deserialize(obj["sortOrder"]);
  return std::make_shared<HiveSortingColumn>(columnName, sortOrder);
}

std::string HiveSortingColumn::toString() const {
  return fmt::format(
      "[COLUMN[{}] ORDER[{}]]", sortColumn_, sortOrder_.toString());
}

void HiveSortingColumn::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveSortingColumn", HiveSortingColumn::deserialize);
}

HiveBucketProperty::HiveBucketProperty(
    Kind kind,
    int32_t bucketCount,
    const std::vector<std::string>& bucketedBy,
    const std::vector<TypePtr>& bucketTypes,
    const std::vector<std::shared_ptr<const HiveSortingColumn>>& sortedBy)
    : kind_(kind),
      bucketCount_(bucketCount),
      bucketedBy_(bucketedBy),
      bucketTypes_(bucketTypes),
      sortedBy_(sortedBy) {
  validate();
}

void HiveBucketProperty::validate() const {
  VELOX_USER_CHECK_GT(bucketCount_, 0, "Hive bucket count can't be zero");
  VELOX_USER_CHECK(!bucketedBy_.empty(), "Hive bucket columns must be set");
  VELOX_USER_CHECK_EQ(
      bucketedBy_.size(),
      bucketTypes_.size(),
      "The number of hive bucket columns and types do not match {}",
      toString());
}

std::string HiveBucketProperty::kindString(Kind kind) {
  switch (kind) {
    case Kind::kHiveCompatible:
      return "HIVE_COMPATIBLE";
    case Kind::kPrestoNative:
      return "PRESTO_NATIVE";
    default:
      return fmt::format("UNKNOWN {}", static_cast<int>(kind));
  }
}

folly::dynamic HiveBucketProperty::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveBucketProperty";
  obj["kind"] = static_cast<int64_t>(kind_);
  obj["bucketCount"] = bucketCount_;
  obj["bucketedBy"] = ISerializable::serialize(bucketedBy_);
  obj["bucketedTypes"] = ISerializable::serialize(bucketTypes_);
  obj["sortedBy"] = ISerializable::serialize(sortedBy_);
  return obj;
}

std::shared_ptr<HiveBucketProperty> HiveBucketProperty::deserialize(
    const folly::dynamic& obj,
    void* context) {
  const Kind kind = static_cast<Kind>(obj["kind"].asInt());
  const int32_t bucketCount = obj["bucketCount"].asInt();
  const auto buckectedBy =
      ISerializable::deserialize<std::vector<std::string>>(obj["bucketedBy"]);
  const auto bucketedTypes = ISerializable::deserialize<std::vector<Type>>(
      obj["bucketedTypes"], context);
  const auto sortedBy =
      ISerializable::deserialize<std::vector<HiveSortingColumn>>(
          obj["sortedBy"], context);
  return std::make_shared<HiveBucketProperty>(
      kind, bucketCount, buckectedBy, bucketedTypes, sortedBy);
}

void HiveBucketProperty::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register("HiveBucketProperty", HiveBucketProperty::deserialize);
}

std::string HiveBucketProperty::toString() const {
  std::stringstream out;
  out << "\nHiveBucketProperty[<" << kind_ << " " << bucketCount_ << ">\n";
  out << "\tBucket Columns:\n";
  for (const auto& column : bucketedBy_) {
    out << "\t\t" << column << "\n";
  }
  out << "\tBucket Types:\n";
  for (const auto& type : bucketTypes_) {
    out << "\t\t" << type->toString() << "\n";
  }
  if (!sortedBy_.empty()) {
    out << "\tSortedBy Columns:\n";
    for (const auto& sortColum : sortedBy_) {
      out << "\t\t" << sortColum->toString() << "\n";
    }
  }
  out << "]\n";
  return out.str();
}

HiveInsertTableHandle::HiveInsertTableHandle(
    std::vector<std::shared_ptr<const HiveColumnHandle>> inputColumns,
    std::shared_ptr<const LocationHandle> locationHandle,
    dwio::common::FileFormat storageFormat,
    std::shared_ptr<const HiveBucketProperty> bucketProperty,
    std::optional<common::CompressionKind> compressionKind,
    const std::unordered_map<std::string, std::string>& serdeParameters,
    const std::shared_ptr<dwio::common::WriterOptions>& writerOptions,
    // When this option is set the HiveDataSink will always write a file even
    // if there's no data. This is useful when the table is bucketed, but the
    // engine handles ensuring a 1 to 1 mapping from task to bucket.
    const bool ensureFiles,
    std::shared_ptr<const FileNameGenerator> fileNameGenerator)
    : inputColumns_(std::move(inputColumns)),
      locationHandle_(std::move(locationHandle)),
      storageFormat_(storageFormat),
      bucketProperty_(std::move(bucketProperty)),
      compressionKind_(compressionKind),
      serdeParameters_(serdeParameters),
      writerOptions_(writerOptions),
      ensureFiles_(ensureFiles),
      fileNameGenerator_(std::move(fileNameGenerator)),
      partitionChannels_(computePartitionChannels(inputColumns_)),
      nonPartitionChannels_(computeNonPartitionChannels(inputColumns_)) {
  if (compressionKind.has_value()) {
    VELOX_CHECK(
        compressionKind.value() != common::CompressionKind_MAX,
        "Unsupported compression type: CompressionKind_MAX");
  }

  if (ensureFiles_) {
    // If ensureFiles is set and either the bucketProperty is set or some
    // partition keys are in the data, there is not a 1:1 mapping from Task to
    // files so we can't proactively create writers.
    VELOX_CHECK(
        bucketProperty_ == nullptr || bucketProperty_->bucketCount() == 0,
        "ensureFiles is not supported with bucketing");

    for (const auto& inputColumn : inputColumns_) {
      VELOX_CHECK(
          !inputColumn->isPartitionKey(),
          "ensureFiles is not supported with partition keys in the data");
    }
  }
}

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig)
    : HiveDataSink(
          inputType,
          insertTableHandle,
          connectorQueryCtx,
          commitStrategy,
          hiveConfig,
          getBucketCount(insertTableHandle->bucketProperty()),
          getBucketCount(insertTableHandle->bucketProperty()) > 0
              ? createBucketFunction(
                    *insertTableHandle->bucketProperty(),
                    inputType)
              : nullptr,
          insertTableHandle->partitionChannels(),
          insertTableHandle->nonPartitionChannels(),
          createPartitionIdGenerator(
              inputType,
              insertTableHandle,
              hiveConfig,
              connectorQueryCtx)) {}

HiveDataSink::HiveDataSink(
    RowTypePtr inputType,
    std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx* connectorQueryCtx,
    CommitStrategy commitStrategy,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    uint32_t bucketCount,
    std::unique_ptr<core::PartitionFunction> bucketFunction,
    const std::vector<column_index_t>& partitionChannels,
    const std::vector<column_index_t>& dataChannels,
    std::unique_ptr<PartitionIdGenerator> partitionIdGenerator)
    : inputType_(std::move(inputType)),
      insertTableHandle_(std::move(insertTableHandle)),
      connectorQueryCtx_(connectorQueryCtx),
      commitStrategy_(commitStrategy),
      hiveConfig_(hiveConfig),
      updateMode_(getUpdateMode()),
      maxOpenWriters_(hiveConfig_->maxPartitionsPerWriters(
          connectorQueryCtx->sessionProperties())),
      partitionChannels_(partitionChannels),
      partitionIdGenerator_(std::move(partitionIdGenerator)),
      dataChannels_(dataChannels),
      bucketCount_(static_cast<int32_t>(bucketCount)),
      bucketFunction_(std::move(bucketFunction)),
      partitionKeyAsLowerCase_(hiveConfig_->isPartitionPathAsLowerCase(
          connectorQueryCtx_->sessionProperties())) {
  fileSystemStats_ = std::make_unique<IoStats>();

  if (isBucketed()) {
    VELOX_USER_CHECK_LT(
        bucketCount_,
        hiveConfig_->maxBucketCount(connectorQueryCtx->sessionProperties()),
        "bucketCount exceeds the limit");
  }
  VELOX_USER_CHECK(
      (commitStrategy_ == CommitStrategy::kNoCommit) ||
          (commitStrategy_ == CommitStrategy::kTaskCommit),
      "Unsupported commit strategy: {}",
      CommitStrategyName::toName(commitStrategy_));

  logicalWriterFactory_ = std::make_unique<LogicalWriterFactory>(
      inputType_,
      insertTableHandle_,
      connectorQueryCtx_,
      hiveConfig_,
      updateMode_,
      isCommitRequired(),
      dataChannels_,
      [this](const WriterId& id) -> std::optional<std::string> {
        if (!isPartitioned() || !id.partitionId.has_value()) {
          return std::nullopt;
        }
        return getPartitionName(id.partitionId.value());
      },
      [this](
          const WriterInfo& writerInfo,
          const std::shared_ptr<dwio::common::WriterOptions>& options) {
        customizeWriterOptions(writerInfo, options);
      },
      [this](
          uint32_t writerIndex,
          std::optional<FileInfo> fileInfo,
          std::unique_ptr<dwio::common::FileMetadata> metadata) {
        onFileClosed(writerIndex, std::move(fileInfo), std::move(metadata));
      },
      fileSystemStats_.get());

  partitionWriter_ = std::make_unique<FanoutPartitionWriter>(
      maxOpenWriters_,
      logicalWriterFactory_.get(),
      [this](const WriterId& id, uint32_t writerIndex) {
        onWriterCreated(id, writerIndex);
      },
      connectorQueryCtx_->memoryPool());

  if (insertTableHandle_->ensureFiles()) {
    VELOX_CHECK(
        !isPartitioned() && !isBucketed(),
        "ensureFiles is not supported with bucketing or partition keys in the data");
    partitionWriter_->ensureWriter(WriterId::unpartitionedId());
  }
}

HiveDataSink::~HiveDataSink() = default;

bool HiveDataSink::canReclaim() const {
  return logicalWriterFactory_->canReclaim();
}

void HiveDataSink::appendData(RowVectorPtr input) {
  checkRunning();

  // Lazy load all the input columns.
  input->loadedVector();

  // Write to unpartitioned (and unbucketed) table.
  if (!isPartitioned() && !isBucketed()) {
    partitionWriter_->write(
        WriterId::unpartitionedId(), makeDataInput(dataChannels_, input));
    return;
  }

  // Compute partition and bucket numbers.
  computePartitionAndBucketIds(input);
  auto dataInput = makeDataInput(dataChannels_, input);

  // All inputs belong to a single non-bucketed partition. The partition id
  // must be zero.
  if (!isBucketed() && partitionIdGenerator_->numPartitions() == 1) {
    partitionWriter_->write(WriterId{0}, dataInput);
    return;
  }

  partitionWriter_->write(
      dataInput, partitionIds_, bucketIds_, isPartitioned(), isBucketed());
}

std::string HiveDataSink::stateString(State state) {
  switch (state) {
    case State::kRunning:
      return "RUNNING";
    case State::kFinishing:
      return "FLUSHING";
    case State::kClosed:
      return "CLOSED";
    case State::kAborted:
      return "ABORTED";
    default:
      VELOX_UNREACHABLE("BAD STATE: {}", static_cast<int>(state));
  }
}

void HiveDataSink::computePartitionAndBucketIds(const RowVectorPtr& input) {
  VELOX_CHECK(isPartitioned() || isBucketed());
  if (isPartitioned()) {
    if (!hiveConfig_->allowNullPartitionKeys(
            connectorQueryCtx_->sessionProperties())) {
      // Check that there are no nulls in the partition keys.
      for (auto& partitionIdx : partitionChannels_) {
        auto col = input->childAt(partitionIdx);
        if (col->mayHaveNulls()) {
          for (auto i = 0; i < col->size(); ++i) {
            VELOX_USER_CHECK(
                !col->isNullAt(i),
                "Partition key must not be null: {}",
                input->type()->asRow().nameOf(partitionIdx));
          }
        }
      }
    }
    partitionIdGenerator_->run(input, partitionIds_);
  }

  if (isBucketed()) {
    bucketFunction_->partition(*input, bucketIds_);
  }
}

DataSink::Stats HiveDataSink::stats() const {
  Stats stats;
  if (state_ == State::kAborted) {
    return stats;
  }

  const auto& writers = partitionWriter_->writers();
  for (const auto& writer : writers) {
    stats.numWrittenBytes += writer->ioStats()->rawBytesWritten();
    stats.writeIOTimeUs += writer->ioStats()->writeIOTimeUs();
  }

  if (state_ != State::kClosed) {
    return stats;
  }

  stats.numWrittenFiles = 0;
  for (const auto& writer : writers) {
    const auto& info = writer->writerInfo();
    VELOX_CHECK_NOT_NULL(info);
    stats.numWrittenFiles += info->writtenFiles.size();
    if (!info->spillStats->empty()) {
      stats.spillStats += *info->spillStats;
    }
  }
  return stats;
}

std::unordered_map<std::string, RuntimeCounter> HiveDataSink::runtimeStats()
    const {
  std::unordered_map<std::string, RuntimeCounter> runtimeStats;

  const auto fsStatsMap = fileSystemStats_->stats();
  for (const auto& [statName, statValue] : fsStatsMap) {
    runtimeStats.emplace(
        statName, RuntimeCounter(statValue.sum, statValue.unit));
  }

  return runtimeStats;
}

void HiveDataSink::setState(State newState) {
  checkStateTransition(state_, newState);
  state_ = newState;
}

/// Validates the state transition from 'oldState' to 'newState'.
void HiveDataSink::checkStateTransition(State oldState, State newState) {
  switch (oldState) {
    case State::kRunning:
      if (newState == State::kAborted || newState == State::kFinishing) {
        return;
      }
      break;
    case State::kFinishing:
      if (newState == State::kAborted || newState == State::kClosed ||
          // The finishing state is reentry state if we yield in the middle of
          // finish processing if a single run takes too long.
          newState == State::kFinishing) {
        return;
      }
      [[fallthrough]];
    case State::kAborted:
    case State::kClosed:
    default:
      break;
  }
  VELOX_FAIL("Unexpected state transition from {} to {}", oldState, newState);
}

bool HiveDataSink::finish() {
  setState(State::kFinishing);
  return partitionWriter_->finish();
}

std::vector<std::string> HiveDataSink::close() {
  setState(State::kClosed);
  closeInternal();
  return commitMessage();
}

std::vector<std::string> HiveDataSink::commitMessage() const {
  const auto& writers = partitionWriter_->writers();
  std::vector<std::string> partitionUpdates;
  partitionUpdates.reserve(writers.size());
  for (auto i = 0; i < writers.size(); ++i) {
    const auto& info = writers.at(i)->writerInfo();
    VELOX_CHECK_NOT_NULL(info);

    folly::dynamic fileWriteInfosArray = folly::dynamic::array;
    for (const auto& fileInfo : info->writtenFiles) {
      fileWriteInfosArray.push_back(
          folly::dynamic::object(
              HiveCommitMessage::kWriteFileName, fileInfo.writeFileName)(
              HiveCommitMessage::kTargetFileName, fileInfo.targetFileName)(
              HiveCommitMessage::kFileSize, fileInfo.fileSize));
    }

    // clang-format off
      auto partitionUpdateJson = folly::toJson(
       folly::dynamic::object
          (HiveCommitMessage::kName, info->writerParameters.partitionName().value_or(""))
          (HiveCommitMessage::kUpdateMode,
            WriterParameters::updateModeToString(
              info->writerParameters.updateMode()))
          (HiveCommitMessage::kWritePath, info->writerParameters.writeDirectory())
          (HiveCommitMessage::kTargetPath, info->writerParameters.targetDirectory())
          (HiveCommitMessage::kFileWriteInfos, std::move(fileWriteInfosArray))
          (HiveCommitMessage::kRowCount, info->numWrittenRows)
          (HiveCommitMessage::kInMemoryDataSizeInBytes, info->inputSizeInBytes)
          (HiveCommitMessage::kOnDiskDataSizeInBytes, writers.at(i)->ioStats()->rawBytesWritten())
          (HiveCommitMessage::kContainsNumberedFileNames, true));
    // clang-format on
    partitionUpdates.push_back(partitionUpdateJson);
  }
  return partitionUpdates;
}

void HiveDataSink::abort() {
  setState(State::kAborted);
  closeInternal();
}

void HiveDataSink::closeInternal() {
  VELOX_CHECK_NE(state_, State::kRunning);
  VELOX_CHECK_NE(state_, State::kFinishing);

  TestValue::adjust(
      "facebook::velox::connector::hive::HiveDataSink::closeInternal", this);

  if (state_ == State::kClosed) {
    partitionWriter_->close();
  } else {
    partitionWriter_->abort();
  }
}

std::string HiveDataSink::getPartitionName(uint32_t partitionId) const {
  VELOX_CHECK_NOT_NULL(partitionIdGenerator_);

  return HivePartitionName::partitionName(
      partitionId,
      partitionIdGenerator_->partitionValues(),
      partitionKeyAsLowerCase_);
}

void HiveDataSink::customizeWriterOptions(
    const WriterInfo& /* writerInfo */,
    const std::shared_ptr<dwio::common::WriterOptions>& /* options */) const {}

void HiveDataSink::onWriterCreated(
    const WriterId& /* id */,
    uint32_t /* writerIndex */) {}

void HiveDataSink::onFileClosed(
    uint32_t /* writerIndex */,
    std::optional<FileInfo> /* fileInfo */,
    std::unique_ptr<dwio::common::FileMetadata> /* metadata */) {}

std::pair<std::string, std::string> HiveInsertFileNameGenerator::gen(
    std::optional<uint32_t> bucketId,
    const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx& connectorQueryCtx,
    bool commitRequired) const {
  auto defaultHiveConfig =
      std::make_shared<const HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));

  return this->gen(
      bucketId,
      insertTableHandle,
      connectorQueryCtx,
      defaultHiveConfig,
      commitRequired);
}

std::pair<std::string, std::string> HiveInsertFileNameGenerator::gen(
    std::optional<uint32_t> bucketId,
    const std::shared_ptr<const HiveInsertTableHandle> insertTableHandle,
    const ConnectorQueryCtx& connectorQueryCtx,
    const std::shared_ptr<const HiveConfig>& hiveConfig,
    bool commitRequired) const {
  auto targetFileName = insertTableHandle->locationHandle()->targetFileName();
  const bool generateFileName = targetFileName.empty();
  if (bucketId.has_value()) {
    VELOX_CHECK(generateFileName);
    // TODO: add hive.file_renaming_enabled support.
    targetFileName = computeBucketedFileName(
        connectorQueryCtx.queryId(),
        hiveConfig->maxBucketCount(connectorQueryCtx.sessionProperties()),
        bucketId.value());
    // queryId may contain unsafe characters.
    sanitizeFileName(targetFileName);
  } else if (generateFileName) {
    // targetFileName includes planNodeId and Uuid. As a result, different
    // table writers run by the same task driver or the same table writer
    // run in different task tries would have different targetFileNames.
    targetFileName = fmt::format(
        "{}_{}_{}_{}",
        connectorQueryCtx.taskId(),
        connectorQueryCtx.driverId(),
        connectorQueryCtx.planNodeId(),
        makeUuid());
    // taskId, planNodeId may contain unsafe characters.
    sanitizeFileName(targetFileName);
  }
  // do not try to sanitize user provided targetFileName
  VELOX_CHECK(!targetFileName.empty());
  const std::string writeFileName = commitRequired
      ? fmt::format(".tmp.velox.{}_{}", targetFileName, makeUuid())
      : targetFileName;
  if (generateFileName &&
      insertTableHandle->storageFormat() == dwio::common::FileFormat::PARQUET) {
    return {
        fmt::format("{}{}", targetFileName, ".parquet"),
        fmt::format("{}{}", writeFileName, ".parquet")};
  }
  return {targetFileName, writeFileName};
}

void HiveInsertFileNameGenerator::sanitizeFileName(std::string& name) {
  static const re2::RE2 re("[^a-zA-Z0-9._-]");
  re2::RE2::GlobalReplace(&name, re, "_");
}

folly::dynamic HiveInsertFileNameGenerator::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveInsertFileNameGenerator";
  return obj;
}

std::shared_ptr<HiveInsertFileNameGenerator>
HiveInsertFileNameGenerator::deserialize(
    const folly::dynamic& /* obj */,
    void* /* context */) {
  return std::make_shared<HiveInsertFileNameGenerator>();
}

void HiveInsertFileNameGenerator::registerSerDe() {
  auto& registry = DeserializationWithContextRegistryForSharedPtr();
  registry.Register(
      "HiveInsertFileNameGenerator", HiveInsertFileNameGenerator::deserialize);
}

std::string HiveInsertFileNameGenerator::toString() const {
  return "HiveInsertFileNameGenerator";
}

WriterParameters::UpdateMode HiveDataSink::getUpdateMode() const {
  if (insertTableHandle_->isExistingTable()) {
    if (insertTableHandle_->isPartitioned()) {
      const auto insertBehavior = hiveConfig_->insertExistingPartitionsBehavior(
          connectorQueryCtx_->sessionProperties());
      switch (insertBehavior) {
        case HiveConfig::InsertExistingPartitionsBehavior::kOverwrite:
          return WriterParameters::UpdateMode::kOverwrite;
        case HiveConfig::InsertExistingPartitionsBehavior::kError:
          return WriterParameters::UpdateMode::kNew;
        default:
          VELOX_UNSUPPORTED(
              "Unsupported insert existing partitions behavior: {}",
              HiveConfig::insertExistingPartitionsBehaviorString(
                  insertBehavior));
      }
    } else {
      if (hiveConfig_->immutablePartitions()) {
        VELOX_USER_FAIL("Unpartitioned Hive tables are immutable.");
      }
      return WriterParameters::UpdateMode::kAppend;
    }
  } else {
    return WriterParameters::UpdateMode::kNew;
  }
}

bool HiveInsertTableHandle::isPartitioned() const {
  return std::any_of(
      inputColumns_.begin(), inputColumns_.end(), [](auto column) {
        return column->isPartitionKey();
      });
}

const HiveBucketProperty* HiveInsertTableHandle::bucketProperty() const {
  return bucketProperty_.get();
}

bool HiveInsertTableHandle::isBucketed() const {
  return bucketProperty() != nullptr;
}

bool HiveInsertTableHandle::isExistingTable() const {
  return locationHandle_->tableType() == LocationHandle::TableType::kExisting;
}

folly::dynamic HiveInsertTableHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "HiveInsertTableHandle";
  folly::dynamic arr = folly::dynamic::array;
  for (const auto& ic : inputColumns_) {
    arr.push_back(ic->serialize());
  }

  obj["inputColumns"] = arr;
  obj["locationHandle"] = locationHandle_->serialize();
  obj["tableStorageFormat"] = dwio::common::toString(storageFormat_);

  if (bucketProperty_) {
    obj["bucketProperty"] = bucketProperty_->serialize();
  }

  if (compressionKind_.has_value()) {
    obj["compressionKind"] = common::compressionKindToString(*compressionKind_);
  }

  folly::dynamic params = folly::dynamic::object;
  for (const auto& [key, value] : serdeParameters_) {
    params[key] = value;
  }
  obj["serdeParameters"] = params;
  obj["ensureFiles"] = ensureFiles_;
  obj["fileNameGenerator"] = fileNameGenerator_->serialize();
  return obj;
}

HiveInsertTableHandlePtr HiveInsertTableHandle::create(
    const folly::dynamic& obj) {
  auto inputColumns = ISerializable::deserialize<std::vector<HiveColumnHandle>>(
      obj["inputColumns"]);
  auto locationHandle =
      ISerializable::deserialize<LocationHandle>(obj["locationHandle"]);
  auto storageFormat =
      dwio::common::toFileFormat(obj["tableStorageFormat"].asString());

  std::optional<common::CompressionKind> compressionKind = std::nullopt;
  if (obj.count("compressionKind") > 0) {
    compressionKind =
        common::stringToCompressionKind(obj["compressionKind"].asString());
  }

  std::shared_ptr<const HiveBucketProperty> bucketProperty;
  if (obj.count("bucketProperty") > 0) {
    bucketProperty =
        ISerializable::deserialize<HiveBucketProperty>(obj["bucketProperty"]);
  }

  std::unordered_map<std::string, std::string> serdeParameters;
  for (const auto& pair : obj["serdeParameters"].items()) {
    serdeParameters.emplace(pair.first.asString(), pair.second.asString());
  }

  bool ensureFiles = obj["ensureFiles"].asBool();

  auto fileNameGenerator =
      ISerializable::deserialize<FileNameGenerator>(obj["fileNameGenerator"]);
  return std::make_shared<HiveInsertTableHandle>(
      inputColumns,
      locationHandle,
      storageFormat,
      bucketProperty,
      compressionKind,
      serdeParameters,
      nullptr, // writerOptions is not serializable
      ensureFiles,
      fileNameGenerator);
}

void HiveInsertTableHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("HiveInsertTableHandle", HiveInsertTableHandle::create);
}

std::string HiveInsertTableHandle::toString() const {
  std::ostringstream out;
  out << "HiveInsertTableHandle [" << dwio::common::toString(storageFormat_);
  if (compressionKind_.has_value()) {
    out << " " << common::compressionKindToString(compressionKind_.value());
  } else {
    out << " none";
  }
  out << "], [inputColumns: [";
  for (const auto& i : inputColumns_) {
    out << " " << i->toString();
  }
  out << " ], locationHandle: " << locationHandle_->toString();
  if (bucketProperty_) {
    out << ", bucketProperty: " << bucketProperty_->toString();
  }

  if (serdeParameters_.size() > 0) {
    std::map<std::string, std::string> sortedSerdeParams(
        serdeParameters_.begin(), serdeParameters_.end());
    out << ", serdeParameters: ";
    for (const auto& [key, value] : sortedSerdeParams) {
      out << "[" << key << ", " << value << "] ";
    }
  }
  out << ", fileNameGenerator: " << fileNameGenerator_->toString();
  out << "]";
  return out.str();
}

std::string LocationHandle::toString() const {
  return fmt::format(
      "LocationHandle [targetPath: {}, writePath: {}, tableType: {}, tableFileName: {}]",
      targetPath_,
      writePath_,
      tableTypeName(tableType_),
      targetFileName_);
}

void LocationHandle::registerSerDe() {
  auto& registry = DeserializationRegistryForSharedPtr();
  registry.Register("LocationHandle", LocationHandle::create);
}

folly::dynamic LocationHandle::serialize() const {
  folly::dynamic obj = folly::dynamic::object;
  obj["name"] = "LocationHandle";
  obj["targetPath"] = targetPath_;
  obj["writePath"] = writePath_;
  obj["tableType"] = tableTypeName(tableType_);
  obj["targetFileName"] = targetFileName_;
  return obj;
}

LocationHandlePtr LocationHandle::create(const folly::dynamic& obj) {
  auto targetPath = obj["targetPath"].asString();
  auto writePath = obj["writePath"].asString();
  auto tableType = tableTypeFromName(obj["tableType"].asString());
  auto targetFileName = obj["targetFileName"].asString();
  return std::make_shared<LocationHandle>(
      targetPath, writePath, tableType, targetFileName);
}
} // namespace facebook::velox::connector::hive
