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

#include <any>
#include <cstdint>
#include <string>
#include "velox/vector/ComplexVector.h"

namespace facebook::velox::connector::hive::iceberg {
#define SECONDS_PER_DAY (86400)
inline const std::string TRANSFORM_IDENTITY = "identity";
inline const std::string TRANSFORM_YEAR = "year";
inline const std::string TRANSFORM_MONTH = "month";
inline const std::string TRANSFORM_DAY = "day";
inline const std::string TRANSFORM_HOUR = "hour";
inline const std::string TRANSFORM_BUCKET = "bucket";
inline const std::string TRANSFORM_TRUNCATE = "truncate";

struct VeloxIcebergSchema;

class ColumnTransform {
 public:
  ColumnTransform(
      const column_index_t columnSourceId,
      const std::string& columnName,
      const std::string& transformName,
      TypePtr resultType,
      std::optional<int32_t> parameter,
      std::function<VectorPtr(const VectorPtr&)> blockTransform)
      : columnSourceId_(columnSourceId),
        columnName_(columnName),
        transform_(std::move(transformName)),
        resultType_(std::move(resultType)),
        parameter_(parameter),
        blockTransform_(std::move(blockTransform)) {}

  const std::string& transformName() const {
    return transform_;
  }

  const TypePtr& resultType() const {
    return resultType_;
  }

  column_index_t columnSourceId() const {
    return columnSourceId_;
  }

  VectorPtr transformBlock(const VectorPtr& block) const {
    return blockTransform_(block);
  }

 private:
  column_index_t columnSourceId_;
  std::string columnName_;
  std::string transform_;
  TypePtr resultType_; // transform output type
  std::optional<int32_t> parameter_;
  std::function<VectorPtr(const VectorPtr&)> blockTransform_;
};

class ColumnTransforms {
 public:
  ColumnTransforms() = default;

  void add(const ColumnTransform& transform) {
    columnTransforms_.emplace_back(transform);
  }

  void add(
      column_index_t columnSourceId,
      const std::string& columnName,
      const std::string& transformName,
      TypePtr resultType,
      std::optional<int32_t> parameter,
      std::function<VectorPtr(const VectorPtr&)> blockTransform) {
    columnTransforms_.emplace_back(
        columnSourceId,
        columnName,
        transformName,
        resultType,
        parameter,
        blockTransform);
  }

  std::vector<column_index_t> getPartitionColumnIds() const {
    std::vector<column_index_t> columnIds;
    columnIds.reserve(columnTransforms_.size());
    std::transform(
        columnTransforms_.begin(),
        columnTransforms_.end(),
        std::back_inserter(columnIds),
        [](const ColumnTransform& transform) {
          return transform.columnSourceId();
        });
    return columnIds;
  }

  const std::vector<ColumnTransform>& getColumnTransforms() const {
    return columnTransforms_;
  }

 private:
  std::vector<ColumnTransform> columnTransforms_;
};

struct VeloxIcebergPartitionSpec {
  const int32_t specId;
  std::shared_ptr<const VeloxIcebergSchema> schema;
  velox::memory::MemoryPool* pool;
  std::vector<std::string> fields;
  std::shared_ptr<ColumnTransforms> columnTransforms;
  VeloxIcebergPartitionSpec(
      const int32_t _specId,
      const std::shared_ptr<const VeloxIcebergSchema>& _schema,
      const std::vector<std::string>& _fields,
      velox::memory::MemoryPool* _pool);
};
} // namespace facebook::velox::connector::hive::iceberg
