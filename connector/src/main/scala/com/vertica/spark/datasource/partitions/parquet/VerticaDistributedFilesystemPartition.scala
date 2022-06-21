// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.datasource.partitions.parquet

import com.vertica.spark.datasource.core.VerticaPartition
import com.vertica.spark.datasource.partitions.{DistributedFilesystemPartition, FilePortion}

/**
 * Partition for distributed filesystem transport method using parquet files
 *
 * @param fileRanges    List of files and ranges of row groups to read for those files
 * @param rangeCountMap Map representing how many file ranges exist for each file. Used for tracking and cleanup.
 */
final case class VerticaDistributedFilesystemPartition(fileRanges: Seq[ParquetFileRange], rangeCountMap: Map[String, Int])
  extends VerticaPartition with DistributedFilesystemPartition {
  override def getFilePortions: Seq[FilePortion] = this.fileRanges

  override def getPartitioningRecord: Map[String, Int] = this.rangeCountMap
}
