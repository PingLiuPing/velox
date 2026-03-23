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

namespace facebook::velox::connector::hive {

/// Defines the JSON field names used in Hive commit messages.
struct HiveCommitMessage {
  // Stores the JSON field name for the Hive partition name.
  static constexpr const char* kName = "name";
  // Stores the JSON field name for the update mode.
  static constexpr const char* kUpdateMode = "updateMode";
  // Stores the JSON field name for the staging directory path.
  static constexpr const char* kWritePath = "writePath";
  // Stores the JSON field name for the final directory path.
  static constexpr const char* kTargetPath = "targetPath";
  // Stores the JSON field name for the array of file descriptors.
  static constexpr const char* kFileWriteInfos = "fileWriteInfos";
  // Stores the JSON field name for the staging file name.
  static constexpr const char* kWriteFileName = "writeFileName";
  // Stores the JSON field name for the final file name.
  static constexpr const char* kTargetFileName = "targetFileName";
  // Stores the JSON field name for the file size in bytes.
  static constexpr const char* kFileSize = "fileSize";
  // Stores the JSON field name for the total row count.
  static constexpr const char* kRowCount = "rowCount";
  // Stores the JSON field name for the logical input size in bytes.
  static constexpr const char* kInMemoryDataSizeInBytes =
      "inMemoryDataSizeInBytes";
  // Stores the JSON field name for the physical output size in bytes.
  static constexpr const char* kOnDiskDataSizeInBytes = "onDiskDataSizeInBytes";
  // Stores the JSON field name indicating numbered file naming.
  static constexpr const char* kContainsNumberedFileNames =
      "containsNumberedFileNames";
};

} // namespace facebook::velox::connector::hive
