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

#include <common/encode/Base64.h>
#include <regex>
#include "PartitionTransforms.h"
#include "IcebergDataSink.h"
#include "Murmur3.h"
#include "velox/type/Type.h"

namespace facebook::velox::connector::hive::iceberg {
static int32_t
offsetOfNthCodePoint(const char* data, const size_t len, const size_t n) {
  int32_t i{0};
  int32_t count{0}; // number of code points seen

  while (i < len) {
    if (count == n) {
      return i;
    }
    const unsigned char c = data[i];
    size_t charLen = 0;
    // 1-byte ASCII
    if (c < 0x80) {
      charLen = 1;
    } else if ((c & 0xE0) == 0xC0) {
      charLen = 2;
    } else if ((c & 0xF0) == 0xE0) {
      charLen = 3;
    } else if ((c & 0xF8) == 0xF0) {
      charLen = 4;
    }
    i += charLen;
    ++count;
  }
  return (count >= n) ? i : len;
}
static int32_t epochYear(const int64_t daysSinceEpoch) {
  const std::time_t seconds =
      static_cast<std::time_t>(daysSinceEpoch) * SECONDS_PER_DAY;
  const std::tm* timeInfo = std::gmtime(&seconds);
  if (!timeInfo) {
    VELOX_NYI(fmt::format("Cannot convert {} to epoch year.", daysSinceEpoch));
  }
  // tm_year is the number of years since 1900.
  return timeInfo->tm_year + 1900;
}

static int32_t epochMonth(const int64_t daysSinceEpoch) {
  const std::time_t seconds =
      static_cast<std::time_t>(daysSinceEpoch) * SECONDS_PER_DAY;
  const std::tm* timeInfo = std::gmtime(&seconds);
  if (!timeInfo) {
    VELOX_NYI(fmt::format("Cannot convert {} to epoch month.", daysSinceEpoch));
  }
  // tm_mon start from 0
  return timeInfo->tm_mon + 1;
}

static int32_t epochDay(const int64_t daysSinceEpoch) {
  const std::time_t seconds =
      static_cast<std::time_t>(daysSinceEpoch) * SECONDS_PER_DAY;
  const std::tm* timeInfo = std::gmtime(&seconds);
  if (!timeInfo) {
    VELOX_NYI(fmt::format("Cannot convert {} to epoch day.", daysSinceEpoch));
  }
  return timeInfo->tm_mday;
}

template <TypeKind iKind, TypeKind oKind, typename Func>
VectorPtr transformBlock(
    const VectorPtr& block,
    const TypePtr output,
    const Func transform,
    velox::memory::MemoryPool* pool) {
  using TOUT = typename TypeTraits<oKind>::NativeType;
  using TIN = typename TypeTraits<iKind>::NativeType;
  VectorPtr base = BaseVector::create(output, block->size(), pool);
  auto transformed = std::dynamic_pointer_cast<FlatVector<TOUT>>(base);
  if (output->isBoolean()) {
    auto simpleVector = block->as<SimpleVector<TIN>>();
    for (auto i = 0; i < block->size(); ++i) {
      if (block->isNullAt(i)) {
        transformed->setNull(i, true);
      } else {
        transformed->set(i, transform(simpleVector->valueAt(i)));
      }
    }
  } else {
    auto flatVector = block->as<FlatVector<TIN>>();
    for (auto i = 0; i < block->size(); ++i) {
      if (block->isNullAt(i)) {
        transformed->setNull(i, true);
      } else {
        transformed->set(i, transform(flatVector->valueAt(i)));
      }
    }
  }
  return base;
}

template <TypeKind Kind>
ColumnTransform createIdentityTransform(
    const int32_t columnSourceId,
    const std::string columnName,
    const std::string& transformName,
    const std::shared_ptr<const Type>& type,
    velox::memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<Kind>::NativeType;

  auto identity = [=](const NativeType value) -> NativeType {
    if constexpr (Kind == TypeKind::VARBINARY) {
      std::string encodedValue =
          encoding::Base64::encode(value.data(), value.size());
      return StringView(encodedValue);
    }
    return value;
  };
  return ColumnTransform(
      columnSourceId,
      columnName,
      transformName,
      type,
      std::nullopt,
      [=](const VectorPtr& block) -> VectorPtr {
        return transformBlock<Kind, Kind>(block, type, identity, pool);
      });
}

template <typename T, TypeKind Kind>
ColumnTransform createBucketTransform(
    const int32_t columnSourceId,
    const std::string& columnName,
    const std::string& transformName,
    const std::shared_ptr<const Type>& type,
    const int32_t count,
    velox::memory::MemoryPool* pool) {
  auto bucketLambda = [=](T value) -> int32_t {
    uint32_t hashVal;
    if constexpr (std::is_same_v<T, int64_t>) {
      if (type->isShortDecimal()) {
        hashVal = connectors::hive::iceberg::Murmur3_32::hashDecimal(value);
      } else {
        hashVal = connectors::hive::iceberg::Murmur3_32::hash(value);
      }
    } else {
      hashVal = connectors::hive::iceberg::Murmur3_32::hash(value);
    }
    return (hashVal & 0x7fffffff) % count;
  };

  return ColumnTransform(
      columnSourceId,
      columnName,
      transformName,
      INTEGER(), // output of bucket transform is always an integer
      std::nullopt,
      [=](const VectorPtr& block) -> VectorPtr {
        return transformBlock<Kind, TypeKind::INTEGER>(
            block, INTEGER(), bucketLambda, pool);
      });
}

template <TypeKind Kind>
ColumnTransform createTruncateTransform(
    const int32_t columnSourceId,
    const std::string& columnName,
    const std::string& transformName,
    const std::shared_ptr<const Type>& type,
    const int32_t width,
    velox::memory::MemoryPool* pool) {
  using NativeType = typename TypeTraits<Kind>::NativeType;

  auto truncateLambda = [=](const NativeType value) -> NativeType {
    if constexpr (Kind == TypeKind::INTEGER || Kind == TypeKind::BIGINT) {
      return value - (((value % width) + width) % width);
    } else if constexpr (Kind == TypeKind::VARCHAR) {
      const size_t offset =
          offsetOfNthCodePoint(value.data(), value.size(), width);
      return StringView(value.data(), offset);
    } else if constexpr (Kind == TypeKind::VARBINARY) {
      const std::string encodedValue = encoding::Base64::encode(
          value.data(), width > value.size() ? value.size() : width);
      return StringView(encodedValue);
    }
  };

  return ColumnTransform(
      columnSourceId,
      columnName,
      transformName,
      type,
      std::nullopt,
      [=](const VectorPtr& block) -> VectorPtr {
        return transformBlock<Kind, Kind>(block, type, truncateLambda, pool);
      });
}

static ColumnTransform buildColumnTransform(
    const column_index_t columnSourceId,
    const std::string& columnName,
    const std::string& transformName,
    const TypePtr& type,
    std::optional<int32_t> parameter,
    velox::memory::MemoryPool* pool) {
  // Identity transform
  if (transformName == TRANSFORM_IDENTITY) {
    if (type->isInteger()) {
      return createIdentityTransform<TypeKind::INTEGER>(
          columnSourceId, columnName, transformName, type, pool);
    }
    if (type->isBigint() || type->isShortDecimal()) {
      return createIdentityTransform<TypeKind::BIGINT>(
          columnSourceId, columnName, transformName, type, pool);
    }
    if (type->isVarchar()) {
      return createIdentityTransform<TypeKind::VARCHAR>(
          columnSourceId, columnName, transformName, type, pool);
    }
    if (type->isVarbinary()) {
      return createIdentityTransform<TypeKind::VARBINARY>(
          columnSourceId, columnName, transformName, type, pool);
    }
    if (type->isBoolean()) {
      return createIdentityTransform<TypeKind::BOOLEAN>(
          columnSourceId, columnName, transformName, type, pool);
    }
    if (type->isLongDecimal()) {
      return createIdentityTransform<TypeKind::HUGEINT>(
          columnSourceId, columnName, transformName, type, pool);
    }
    VELOX_NYI(
        fmt::format(
            "Unsupported column type {} for transform {}",
            type->name(),
            transformName));
  }
  // Year transform
  if (transformName == TRANSFORM_YEAR) {
    auto transformYear = [](const int64_t value) -> int32_t {
      return epochYear(value);
    };
    if (type->isDate()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::INTEGER, TypeKind::INTEGER>(
                block, INTEGER(), transformYear, pool);
          });
    }
    if (type->isTimestamp()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::BIGINT, TypeKind::INTEGER>(
                block, INTEGER(), transformYear, pool);
          });
    }
    VELOX_NYI(
        fmt::format(
            "Unsupported column type {} for transform {}",
            type->name(),
            transformName));
  }
  // Month transform
  if (transformName == TRANSFORM_MONTH) {
    auto transformMonth = [](const int64_t value) -> int32_t {
      return epochMonth(value);
    };
    if (type->isDate()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::INTEGER, TypeKind::INTEGER>(
                block, INTEGER(), transformMonth, pool);
          });
    }
    if (type->isTimestamp()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::BIGINT, TypeKind::INTEGER>(
                block, INTEGER(), transformMonth, pool);
          });
    }
    VELOX_NYI(
        fmt::format(
            "Unsupported column type {} for transform {}",
            type->name(),
            transformName));
  }
  // Day transform
  if (transformName == TRANSFORM_DAY) {
    auto transformDay = [](const int64_t value) -> int32_t {
      return epochDay(value);
    };
    if (type->isDate()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::INTEGER, TypeKind::INTEGER>(
                block, INTEGER(), transformDay, pool);
          });
    }
    if (type->isTimestamp()) {
      return ColumnTransform(
          columnSourceId,
          columnName,
          transformName,
          INTEGER(),
          std::nullopt,
          [=](const VectorPtr& block) -> VectorPtr {
            return transformBlock<TypeKind::BIGINT, TypeKind::INTEGER>(
                block, INTEGER(), transformDay, pool);
          });
    }
    VELOX_NYI(
        fmt::format(
            "Unsupported column type {} for transform {}",
            type->name(),
            transformName));
  }
  // Hour transform
  if (transformName == TRANSFORM_HOUR) {
    VELOX_NYI("Unsupported partition transform.");
  }
  // Bucket transform
  if (transformName == TRANSFORM_BUCKET) {
    if (parameter.has_value()) {
      const int32_t count = parameter.value();
      if (type->isInteger()) {
        return createBucketTransform<int32_t, TypeKind::INTEGER>(
            columnSourceId, columnName, transformName, type, count, pool);
      }
      if (type->isBigint() || type->isShortDecimal()) {
        return createBucketTransform<int64_t, TypeKind::BIGINT>(
            columnSourceId, columnName, transformName, type, count, pool);
      }
      if (type->isVarchar()) {
        return createBucketTransform<StringView, TypeKind::VARCHAR>(
            columnSourceId, columnName, transformName, type, count, pool);
      }
      if (type->isVarbinary()) {
        return createBucketTransform<StringView, TypeKind::VARBINARY>(
            columnSourceId, columnName, transformName, type, count, pool);
      }
      VELOX_NYI(
          fmt::format(
              "Unsupported column type {} for transform {}",
              type->name(),
              transformName));
    } else {
      VELOX_FAIL("Bucket transform requires a number of buckets parameter.");
    }
  }

  // Truncate transform
  if (transformName == TRANSFORM_TRUNCATE) {
    if (parameter.has_value()) {
      const int32_t width = parameter.value();
      if (type->isInteger()) {
        return createTruncateTransform<TypeKind::INTEGER>(
            columnSourceId, columnName, transformName, type, width, pool);
      }
      if (type->isBigint() || type->isShortDecimal()) {
        return createTruncateTransform<TypeKind::BIGINT>(
            columnSourceId, columnName, transformName, type, width, pool);
      }
      if (type->isVarchar()) {
        return createTruncateTransform<TypeKind::VARCHAR>(
            columnSourceId, columnName, transformName, type, width, pool);
      }
      if (type->isVarbinary()) {
        return createTruncateTransform<TypeKind::VARBINARY>(
            columnSourceId, columnName, transformName, type, width, pool);
      }
      VELOX_NYI(
          fmt::format(
              "Unsupported column type {} for transform {}",
              type->name(),
              transformName));
    } else {
      VELOX_FAIL("Truncate transform requires width parameter.");
    }
  }
  VELOX_FAIL(fmt::format("Unsupported transform {}."), transformName);
}

static std::pair<int32_t, TypePtr> getColumnMetaData(
    const std::string& columnName,
    const std::shared_ptr<const VeloxIcebergSchema>& schema) {
  for (const auto column : schema->columns) {
    if (columnName == column->name) {
      const TypePtr typePtr = column->prestoType;
      return {column->id, typePtr};
    }
  }
  return {-1, nullptr};
}
static std::shared_ptr<ColumnTransforms> parsePartitionTransformSpecs(
    const std::vector<std::string>& fields,
    std::shared_ptr<const VeloxIcebergSchema> schema,
    velox::memory::MemoryPool* pool) {
  const std::string namePattern = "([a-z_][a-z0-9_]*)";
  std::regex identityRegex("^" + namePattern + "$");
  std::regex yearRegex("^(year)\\(" + namePattern + "\\)$");
  std::regex monthRegex("^(month)\\(" + namePattern + "\\)$");
  std::regex dayRegex("^(day)\\(" + namePattern + "\\)$");
  std::regex hourRegex("^(hour)\\(" + namePattern + "\\)$");
  std::regex bucketRegex("^(bucket)\\(" + namePattern + ", *([0-9]+)\\)$");
  std::regex truncateRegex("^(truncate)\\(" + namePattern + ", *([0-9]+)\\)$");

  auto columnTransforms = std::make_shared<ColumnTransforms>();
  for (const auto& field : fields) {
    if (std::smatch match; std::regex_match(field, match, yearRegex) ||
        std::regex_match(field, match, monthRegex) ||
        std::regex_match(field, match, dayRegex) ||
        std::regex_match(field, match, hourRegex)) {
      auto [id, type] = getColumnMetaData(match[2].str(), schema);
      columnTransforms->add(buildColumnTransform(
          id, match[2].str(), match[1].str(), type, std::nullopt, pool));
    } else if (
        std::regex_match(field, match, bucketRegex) ||
        std::regex_match(field, match, truncateRegex)) {
      int32_t param = std::stoi(match[3].str());
      auto [id, type] = getColumnMetaData(match[2].str(), schema);
      columnTransforms->add(buildColumnTransform(
          id, match[2].str(), match[1].str(), type, param, pool));
    } else if (std::regex_match(field, match, identityRegex)) {
      auto [id, type] = getColumnMetaData(match[1].str(), schema);
      columnTransforms->add(buildColumnTransform(
          id, match[1].str(), "identity", type, std::nullopt, pool));
    } else {
      VELOX_NYI("Invalid partition transform: " + field);
    }
  }
  return columnTransforms;
}

VeloxIcebergPartitionSpec::VeloxIcebergPartitionSpec(
    const int32_t _specId,
    const std::shared_ptr<const VeloxIcebergSchema>& _schema,
    const std::vector<std::string>& _fields,
    velox::memory::MemoryPool* _pool)
    : specId(_specId), schema(_schema), pool(_pool), fields(_fields) {
  columnTransforms = parsePartitionTransformSpecs(fields, schema, pool);
}
} // namespace facebook::velox::connector::hive::iceberg
