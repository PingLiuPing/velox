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

#include "velox/exec/fuzzer/TopNRowNumberFuzzer.h"
#include <boost/random/uniform_int_distribution.hpp>
#include <utility>
#include "velox/common/file/FileSystems.h"
#include "velox/connectors/hive/HiveConnector.h"
#include "velox/connectors/hive/HiveConnectorSplit.h"
#include "velox/dwio/dwrf/RegisterDwrfReader.h"
#include "velox/exec/fuzzer/FuzzerUtil.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/exec/tests/utils/AssertQueryBuilder.h"
#include "velox/exec/tests/utils/PlanBuilder.h"
#include "velox/exec/tests/utils/TempDirectoryPath.h"
#include "velox/serializers/CompactRowSerializer.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorMaker.h"

DEFINE_int32(steps, 10, "Number of plans to generate and test.");

DEFINE_int32(
    duration_sec,
    0,
    "For how long it should run (in seconds). If zero, "
    "it executes exactly --steps iterations and exits.");

DEFINE_int32(
    batch_size,
    100,
    "The number of elements on each generated vector.");

DEFINE_int32(num_batches, 10, "The number of generated vectors.");

DEFINE_double(
    null_ratio,
    0.1,
    "Chance of adding a null value in a vector "
    "(expressed as double from 0 to 1).");

DEFINE_bool(enable_spill, true, "Whether to test plans with spilling enabled.");

DEFINE_int32(
    max_spill_level,
    -1,
    "Max spill level, -1 means random [0, 7], otherwise the actual level.");

DEFINE_bool(
    enable_oom_injection,
    false,
    "When enabled OOMs will randomly be triggered while executing query "
    "plans. The goal of this mode is to ensure unexpected exceptions "
    "aren't thrown and the process isn't killed in the process of cleaning "
    "up after failures. Therefore, results are not compared when this is "
    "enabled. Note that this option only works in debug builds.");

namespace facebook::velox::exec::test {
namespace {

class TopNRowNumberFuzzer {
 public:
  explicit TopNRowNumberFuzzer(
      size_t initialSeed,
      std::unique_ptr<ReferenceQueryRunner>);

  void go();

  struct PlanWithSplits {
    core::PlanNodePtr plan;
    std::vector<Split> splits;

    explicit PlanWithSplits(
        core::PlanNodePtr _plan,
        const std::vector<Split>& _splits = {})
        : plan(std::move(_plan)), splits(_splits) {}
  };

 private:
  static VectorFuzzer::Options getFuzzerOptions() {
    VectorFuzzer::Options opts;
    opts.vectorSize = FLAGS_batch_size;
    opts.stringVariableLength = true;
    opts.stringLength = 100;
    opts.nullRatio = FLAGS_null_ratio;
    return opts;
  }

  void seed(size_t seed) {
    currentSeed_ = seed;
    vectorFuzzer_.reSeed(seed);
    rng_.seed(currentSeed_);
  }

  void reSeed() {
    seed(rng_());
  }

  // Runs one test iteration from query plans generations, executions and result
  // verifications.
  void verify();

  int32_t randInt(int32_t min, int32_t max) {
    return boost::random::uniform_int_distribution<int32_t>(min, max)(rng_);
  }

  std::pair<std::vector<std::string>, std::vector<TypePtr>> generateKeys(
      const std::string& prefix);

  std::vector<RowVectorPtr> generateInput(
      const std::vector<std::string>& keyNames,
      const std::vector<TypePtr>& keyTypes,
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortingKeys);

  std::optional<MaterializedRowMultiset> computeReferenceResults(
      core::PlanNodePtr& plan,
      const std::vector<RowVectorPtr>& input);

  RowVectorPtr execute(const PlanWithSplits& plan, bool injectSpill);

  void addPlansWithTableScan(
      const std::string& tableDir,
      const std::vector<std::string>& partitionKeys,
      const std::vector<RowVectorPtr>& input,
      std::vector<PlanWithSplits>& altPlans);

  // Makes the query plan with default settings in TopNRowNumberFuzzer.
  PlanWithSplits makeDefaultPlan(
      const std::vector<std::string>& partitionKeys,
      const std::vector<std::string>& sortKeys,
      const std::vector<std::string>& allKeys,
      const std::vector<RowVectorPtr>& input);

  PlanWithSplits makePlanWithTableScan(
      const RowTypePtr& type,
      const std::vector<std::string>& partitionKeys,
      const std::vector<Split>& splits);

  FuzzerGenerator rng_;
  size_t currentSeed_{0};

  std::shared_ptr<memory::MemoryPool> rootPool_{
      memory::memoryManager()->addRootPool(
          "rowNumberFuzzer",
          memory::kMaxMemory,
          memory::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> pool_{rootPool_->addLeafChild(
      "rowNumberFuzzerLeaf",
      true,
      exec::MemoryReclaimer::create())};
  std::shared_ptr<memory::MemoryPool> writerPool_{rootPool_->addAggregateChild(
      "rowNumberFuzzerWriter",
      exec::MemoryReclaimer::create())};
  VectorFuzzer vectorFuzzer_;
  std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner_;
};

TopNRowNumberFuzzer::TopNRowNumberFuzzer(
    size_t initialSeed,
    std::unique_ptr<ReferenceQueryRunner> referenceQueryRunner)
    : vectorFuzzer_{getFuzzerOptions(), pool_.get()},
      referenceQueryRunner_{std::move(referenceQueryRunner)} {
  filesystems::registerLocalFileSystem();
  dwrf::registerDwrfReaderFactory();

  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kPresto)) {
    serializer::presto::PrestoVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kCompactRow)) {
    serializer::CompactRowVectorSerde::registerNamedVectorSerde();
  }
  if (!isRegisteredNamedVectorSerde(VectorSerde::Kind::kUnsafeRow)) {
    serializer::spark::UnsafeRowVectorSerde::registerNamedVectorSerde();
  }

  // Make sure not to run out of open file descriptors.
  std::unordered_map<std::string, std::string> hiveConfig = {
      {connector::hive::HiveConfig::kNumCacheFileHandles, "1000"}};
  connector::registerConnectorFactory(
      std::make_shared<connector::hive::HiveConnectorFactory>());
  auto hiveConnector =
      connector::getConnectorFactory(
          connector::hive::HiveConnectorFactory::kHiveConnectorName)
          ->newConnector(
              kHiveConnectorId,
              std::make_shared<config::ConfigBase>(std::move(hiveConfig)));
  connector::registerConnector(hiveConnector);

  seed(initialSeed);
}

template <typename T>
bool isDone(size_t i, T startTime) {
  if (FLAGS_duration_sec > 0) {
    std::chrono::duration<double> elapsed =
        std::chrono::system_clock::now() - startTime;
    return elapsed.count() >= FLAGS_duration_sec;
  }
  return i >= FLAGS_steps;
}

std::vector<RowVectorPtr> flatten(const std::vector<RowVectorPtr>& vectors) {
  std::vector<RowVectorPtr> flatVectors;
  for (const auto& vector : vectors) {
    auto flat = BaseVector::create<RowVector>(
        vector->type(), vector->size(), vector->pool());
    flat->copy(vector.get(), 0, 0, vector->size());
    flatVectors.push_back(flat);
  }

  return flatVectors;
}

std::pair<std::vector<std::string>, std::vector<TypePtr>>
TopNRowNumberFuzzer::generateKeys(const std::string& prefix) {
  static const std::vector<TypePtr> kNonFloatingPointTypes{
      BOOLEAN(),
      TINYINT(),
      SMALLINT(),
      INTEGER(),
      BIGINT(),
      VARCHAR(),
      VARBINARY(),
      TIMESTAMP(),
  };

  auto numKeys = boost::random::uniform_int_distribution<uint32_t>(1, 5)(rng_);
  std::vector<std::string> keys;
  std::vector<TypePtr> types;
  for (auto i = 0; i < numKeys; ++i) {
    keys.push_back(fmt::format("{}{}", prefix, i));

    // Pick random, possibly complex, type.
    // if (orderableGroupKeys_) {
    // types.push_back(vectorFuzzer_.randOrderableType(kNonFloatingPointTypes,
    // 2)); } else {
    types.push_back(vectorFuzzer_.randType(kNonFloatingPointTypes, 2));
    //}
  }

  return std::make_pair(keys, types);
}

std::vector<RowVectorPtr> TopNRowNumberFuzzer::generateInput(
    const std::vector<std::string>& keyNames,
    const std::vector<TypePtr>& keyTypes,
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortingKeys) {
  std::vector<RowVectorPtr> input;
  vector_size_t size = vectorFuzzer_.getOptions().vectorSize;
  velox::test::VectorMaker vectorMaker{pool_.get()};
  int64_t rowNumber = 0;

  std::unordered_set<std::string> partitionKeySet{
      partitionKeys.begin(), partitionKeys.end()};
  std::unordered_set<std::string> sortingKeySet{
      sortingKeys.begin(), sortingKeys.end()};

  for (auto j = 0; j < FLAGS_num_batches; ++j) {
    std::vector<VectorPtr> children;

    // Some window functions like 'rank' have semantics influenced by "peer"
    // rows. Peer rows are rows in the same partition having the same order by
    // key. In rank and dense_rank functions, peer rows have the same function
    // result value. This code influences the fuzzer to generate such data.
    //
    // To build such rows the code separates the notions of "peer" groups and
    // "partition" groups during data generation. A number of peers are chosen
    // between (1, size) of the input. Rows with the same peer number have the
    // same order by keys. This means that there are sets of rows in the input
    // data which will have the same order by key.
    //
    // Each peer is then mapped to a partition group. Rows in the same partition
    // group have the same partition keys. So a partition can contain a group of
    // rows with the same order by key and there can be multiple such groups
    // (each with different order by keys) in one partition.
    //
    // This style of data generation is preferable for window functions. The
    // input data so generated could look as follows:
    //
    //   numRows = 6, numPeerGroups = 3, numPartitions = 2,
    //   columns = {p0: VARCHAR, s0: INTEGER}, partitioningKeys = {p0},
    //   sortingKeys = {s0}
    //     row1: 'APPLE'   2
    //     row2: 'APPLE'   2
    //     row3: 'APPLE'   2
    //     row4: 'APPLE'   8
    //     row5: 'ORANGE'  5
    //     row6: 'ORANGE'  5
    //
    // In the above example, the sets of rows belonging to the same peer group
    // are {row1, row2, row3}, {row4}, and {row5, row6}. The sets of rows
    // belonging to the same partition are {row1, row2, row3, row4} and
    // {row5, row6}.
    auto numPeerGroups = size ? randInt(1, size) : 1;
    auto sortingIndices = vectorFuzzer_.fuzzIndices(size, numPeerGroups);
    auto rawSortingIndices = sortingIndices->as<vector_size_t>();
    auto sortingNulls = vectorFuzzer_.fuzzNulls(size);

    auto numPartitions = randInt(1, numPeerGroups);
    auto peerGroupToPartitionIndices =
        vectorFuzzer_.fuzzIndices(numPeerGroups, numPartitions);
    auto rawPeerGroupToPartitionIndices =
        peerGroupToPartitionIndices->as<vector_size_t>();
    auto partitionIndices =
        AlignedBuffer::allocate<vector_size_t>(size, pool_.get());
    auto rawPartitionIndices = partitionIndices->asMutable<vector_size_t>();
    auto partitionNulls = vectorFuzzer_.fuzzNulls(size);
    for (auto i = 0; i < size; i++) {
      auto peerGroup = rawSortingIndices[i];
      rawPartitionIndices[i] = rawPeerGroupToPartitionIndices[peerGroup];
    }

    for (auto i = 0; i < keyTypes.size() - 1; ++i) {
      if (partitionKeySet.find(keyNames[i]) != partitionKeySet.end()) {
        // The partition keys are built with a dictionary over a smaller set of
        // values. This is done to introduce some repetition of key values for
        // windowing.
        auto baseVector = vectorFuzzer_.fuzz(keyTypes[i], numPartitions);
        children.push_back(BaseVector::wrapInDictionary(
            partitionNulls, partitionIndices, size, baseVector));
      } else if (sortingKeySet.find(keyNames[i]) != sortingKeySet.end()) {
        auto baseVector = vectorFuzzer_.fuzz(keyTypes[i], numPeerGroups);
        children.push_back(BaseVector::wrapInDictionary(
            sortingNulls, sortingIndices, size, baseVector));
      } else {
        children.push_back(vectorFuzzer_.fuzz(keyTypes[i], size));
      }
    }
    children.push_back(vectorMaker.flatVector<int32_t>(
        size, [&](auto /*row*/) { return rowNumber++; }));
    input.push_back(vectorMaker.rowVector(keyNames, children));
  }

  return input;
}

TopNRowNumberFuzzer::PlanWithSplits TopNRowNumberFuzzer::makeDefaultPlan(
    const std::vector<std::string>& partitionKeys,
    const std::vector<std::string>& sortKeys,
    const std::vector<std::string>& allKeys,
    const std::vector<RowVectorPtr>& input) {
  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  std::vector<std::string> projectFields = allKeys;
  projectFields.emplace_back("row_number");

  int32_t limit = randInt(0, 100);
  auto plan = PlanBuilder()
                  .values(input)
                  .topNRowNumber(partitionKeys, sortKeys, limit, true)
                  .project(projectFields)
                  .planNode();
  return PlanWithSplits{std::move(plan)};
}

std::optional<MaterializedRowMultiset>
TopNRowNumberFuzzer::computeReferenceResults(
    core::PlanNodePtr& plan,
    const std::vector<RowVectorPtr>& input) {
  if (containsUnsupportedTypes(input[0]->type())) {
    return std::nullopt;
  }

  if (auto sql = referenceQueryRunner_->toSql(plan)) {
    return referenceQueryRunner_->execute(
        sql.value(), input, plan->outputType());
  }

  LOG(INFO) << "Query not supported by the reference DB";
  return std::nullopt;
}

RowVectorPtr TopNRowNumberFuzzer::execute(
    const PlanWithSplits& plan,
    bool injectSpill) {
  LOG(INFO) << "Executing query plan: " << plan.plan->toString(true, true);

  AssertQueryBuilder builder(plan.plan);
  if (!plan.splits.empty()) {
    builder.splits(plan.splits);
  }

  std::shared_ptr<TempDirectoryPath> spillDirectory;
  int32_t spillPct{0};
  if (injectSpill) {
    spillDirectory = exec::test::TempDirectoryPath::create();
    const auto maxSpillLevel =
        FLAGS_max_spill_level == -1 ? randInt(0, 7) : FLAGS_max_spill_level;
    builder.config(core::QueryConfig::kSpillEnabled, true)
        .config(core::QueryConfig::kMaxSpillLevel, maxSpillLevel)
        .config(core::QueryConfig::kTopNRowNumberSpillEnabled, true)
        .spillDirectory(spillDirectory->getPath());
    spillPct = 10;
  }

  ScopedOOMInjector oomInjector(
      []() -> bool { return folly::Random::oneIn(10); },
      10); // Check the condition every 10 ms.
  if (FLAGS_enable_oom_injection) {
    oomInjector.enable();
  }

  // Wait for the task to be destroyed before start next query execution to
  // avoid the potential interference of the background activities across query
  // executions.
  auto stopGuard = folly::makeGuard([&]() { waitForAllTasksToBeDeleted(); });

  TestScopedSpillInjection scopedSpillInjection(spillPct);
  RowVectorPtr result;
  try {
    result = builder.copyResults(pool_.get());
  } catch (VeloxRuntimeError& e) {
    if (FLAGS_enable_oom_injection &&
        e.errorCode() == facebook::velox::error_code::kMemCapExceeded &&
        e.message() == ScopedOOMInjector::kErrorMessage) {
      // If we enabled OOM injection we expect the exception thrown by the
      // ScopedOOMInjector.
      return nullptr;
    }

    throw e;
  }

  if (VLOG_IS_ON(1)) {
    VLOG(1) << std::endl << result->toString(0, result->size());
  }

  return result;
}

TopNRowNumberFuzzer::PlanWithSplits TopNRowNumberFuzzer::makePlanWithTableScan(
    const RowTypePtr& type,
    const std::vector<std::string>& partitionKeys,
    const std::vector<Split>& splits) {
  std::vector<std::string> projectFields = partitionKeys;
  projectFields.emplace_back("row_number");

  auto planNodeIdGenerator = std::make_shared<core::PlanNodeIdGenerator>();
  core::PlanNodeId scanId;
  auto plan = PlanBuilder(planNodeIdGenerator)
                  .tableScan(type)
                  .rowNumber(partitionKeys)
                  .project(projectFields)
                  .planNode();
  return PlanWithSplits{plan, splits};
}

bool isTableScanSupported(const TypePtr& type) {
  if (type->kind() == TypeKind::ROW && type->size() == 0) {
    return false;
  }
  if (type->kind() == TypeKind::UNKNOWN) {
    return false;
  }
  if (type->kind() == TypeKind::HUGEINT) {
    return false;
  }
  // Disable testing with TableScan when input contains TIMESTAMP type, due to
  // the issue #8127.
  if (type->kind() == TypeKind::TIMESTAMP) {
    return false;
  }

  for (auto i = 0; i < type->size(); ++i) {
    if (!isTableScanSupported(type->childAt(i))) {
      return false;
    }
  }

  return true;
}

void TopNRowNumberFuzzer::addPlansWithTableScan(
    const std::string& tableDir,
    const std::vector<std::string>& partitionKeys,
    const std::vector<RowVectorPtr>& input,
    std::vector<PlanWithSplits>& altPlans) {
  VELOX_CHECK(!tableDir.empty());

  if (!isTableScanSupported(input[0]->type())) {
    return;
  }

  const std::vector<Split> inputSplits =
      makeSplits(input, fmt::format("{}/row_number", tableDir), writerPool_);
  altPlans.push_back(makePlanWithTableScan(
      asRowType(input[0]->type()), partitionKeys, inputSplits));
}

void TopNRowNumberFuzzer::verify() {
  const auto [partitionKeys, partitionTypes] = generateKeys("p");
  const auto [sortKeys, sortTypes] = generateKeys("s");
  std::vector<std::string> allSortKeys;
  std::vector<TypePtr> allSortTypes;
  allSortKeys.insert(allSortKeys.begin(), sortKeys.begin(), sortKeys.end());
  allSortTypes.insert(allSortTypes.begin(), sortTypes.begin(), sortTypes.end());
  allSortKeys.push_back("row_id");
  allSortTypes.push_back(INTEGER());
  std::vector<std::string> allKeys;
  std::vector<TypePtr> allTypes;
  allKeys.insert(allKeys.begin(), partitionKeys.begin(), partitionKeys.end());
  allTypes.insert(
      allTypes.begin(), partitionTypes.begin(), partitionTypes.end());
  allKeys.insert(allKeys.end(), allSortKeys.begin(), allSortKeys.end());
  allTypes.insert(allTypes.end(), allSortTypes.begin(), allSortTypes.end());

  const auto input = generateInput(allKeys, allTypes, partitionKeys, sortKeys);

  if (VLOG_IS_ON(1)) {
    // Flatten inputs.
    const auto flatInput = flatten(input);
    VLOG(1) << "Input: " << input[0]->toString();
    for (const auto& v : flatInput) {
      VLOG(1) << std::endl << v->toString(0, v->size());
    }
  }

  auto defaultPlan =
      makeDefaultPlan(partitionKeys, allSortKeys, allKeys, input);

  const auto expected = execute(defaultPlan, /*injectSpill=*/false);

  if (expected != nullptr) {
    if (const auto referenceResult =
            computeReferenceResults(defaultPlan.plan, input)) {
      VELOX_CHECK(
          assertEqualResults(
              referenceResult.value(),
              defaultPlan.plan->outputType(),
              {expected}),
          "Velox and Reference results don't match");
    }
  }

  //  std::vector<PlanWithSplits> altPlans;
  //  altPlans.push_back(std::move(defaultPlan));
  //
  //  const auto tableScanDir = exec::test::TempDirectoryPath::create();
  //  addPlansWithTableScan(tableScanDir->getPath(), partitionKeyNames, input,
  //  altPlans);
  //
  //  for (auto i = 0; i < altPlans.size(); ++i) {
  //    LOG(INFO) << "Testing plan #" << i;
  //    auto actual = execute(altPlans[i], /*injectSpill=*/false);
  //    if (actual != nullptr && expected != nullptr) {
  //      VELOX_CHECK(
  //          assertEqualResults({expected}, {actual}),
  //          "Logically equivalent plans produced different results");
  //    } else {
  //      VELOX_CHECK(
  //          FLAGS_enable_oom_injection, "Got unexpected nullptr for results");
  //    }
  //
  //    if (FLAGS_enable_spill) {
  //      LOG(INFO) << "Testing plan #" << i << " with spilling";
  //      actual = execute(altPlans[i], /*=injectSpill=*/true);
  //      if (actual != nullptr && expected != nullptr) {
  //        try {
  //          VELOX_CHECK(
  //              assertEqualResults({expected}, {actual}),
  //              "Logically equivalent plans produced different results");
  //        } catch (const VeloxException&) {
  //          LOG(ERROR) << "Expected\n"
  //                     << expected->toString(0, expected->size()) <<
  //                     "\nActual\n"
  //                     << actual->toString(0, actual->size());
  //          throw;
  //        }
  //      } else {
  //        VELOX_CHECK(
  //            FLAGS_enable_oom_injection, "Got unexpected nullptr for
  //            results");
  //      }
  //    }
  //  }
}

void TopNRowNumberFuzzer::go() {
  VELOX_USER_CHECK(
      FLAGS_steps > 0 || FLAGS_duration_sec > 0,
      "Either --steps or --duration_sec needs to be greater than zero.");
  VELOX_USER_CHECK_GE(FLAGS_batch_size, 10, "Batch size must be at least 10.");

  const auto startTime = std::chrono::system_clock::now();
  size_t iteration = 0;

  while (!isDone(iteration, startTime)) {
    LOG(INFO) << "==============================> Started iteration "
              << iteration << " (seed: " << currentSeed_ << ")";
    verify();
    LOG(INFO) << "==============================> Done with iteration "
              << iteration;

    reSeed();
    ++iteration;
  }
}
} // namespace

void topNRowNumberFuzzer(
    size_t seed,
    std::unique_ptr<test::ReferenceQueryRunner> referenceQueryRunner) {
  TopNRowNumberFuzzer(seed, std::move(referenceQueryRunner)).go();
}
} // namespace facebook::velox::exec::test
