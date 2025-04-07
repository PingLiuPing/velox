#include "velox/velox/common/memory/MemoryPool.h"
#include "velox/velox/common/memory/Memory.h"
#include "velox/velox/exec/MemoryReclaimer.h"
#include "velox/velox/type/Type.h"
#include "velox/velox/common/config/Config.h"
#include "velox/velox/connectors/Connector.h"
#include "velox/velox/connectors/hive/HiveConfig.h"

namespace facebook::velox::connector::hive {
class HiveConnectorBase {
  public:
   HiveConnectorBase() {
       setupMemoryPools();
   }

  void setupMemoryPools() {
    connectorQueryCtx_.reset();
    connectorPool_.reset();
    opPool_.reset();
    root_.reset();

    root_ = memory::memoryManager()->addRootPool(
        "HiveDataSinkTest", 1L << 30, exec::MemoryReclaimer::create());
    opPool_ = root_->addLeafChild("operator");
    connectorPool_ =
        root_->addAggregateChild("connector", exec::MemoryReclaimer::create());

    connectorQueryCtx_ = std::make_unique<connector::ConnectorQueryCtx>(
        opPool_.get(),
        connectorPool_.get(),
        connectorSessionProperties_.get(),
        nullptr,
        common::PrefixSortConfig(),
        nullptr,
        nullptr,
        "query.HiveDataSinkTest",
        "task.HiveDataSinkTest",
        "planNodeId.HiveDataSinkTest",
        0,
        "");
  }
  std::vector<RowVectorPtr> createNVectors(int vectorSize, int numVectors) {
       VectorFuzzer::Options options;
       options.vectorSize = vectorSize;
       VectorFuzzer fuzzer(options, memoryPool_.get());
       std::vector<RowVectorPtr> vectors;
       for (int i = 0; i < numVectors; ++i) {
           vectors.push_back(fuzzer.fuzzInputRow(rowType_));
       }
       return vectors;
   }
  protected:
  const std::shared_ptr<memory::MemoryPool> memoryPool_ =
   memory::memoryManager()->addLeafPool();

  std::shared_ptr<memory::MemoryPool> root_;
  std::shared_ptr<memory::MemoryPool> opPool_;
  std::shared_ptr<memory::MemoryPool> connectorPool_;
  RowTypePtr rowType_;
  std::shared_ptr<config::ConfigBase> connectorSessionProperties_ =
      std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>(),
          /*mutable=*/true);
  std::unique_ptr<ConnectorQueryCtx> connectorQueryCtx_;
  std::shared_ptr<HiveConfig> connectorConfig_ =
      std::make_shared<HiveConfig>(std::make_shared<config::ConfigBase>(
          std::unordered_map<std::string, std::string>()));
};
}
