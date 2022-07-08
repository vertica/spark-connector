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

package com.vertica.spark.datasource.partitions.file

import com.vertica.spark.datasource.partitions.mixin.{Cleanup, Identifiable}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}

/**
 * Extended from Spark's FilePartition to hold extra hold extra partitioning data.
 *
 * @param partitioningRecords A record of the partition count for all file partition created with the key being the
 *                            file path.
 * */
class VerticaFilePartition(override val index: Int,
                           override val files: Array[PartitionedFile],
                           val filesIdentity: Array[PartitionedFileIdentity],
                           val partitioningRecords: Map[String, Int])
  extends FilePartition(index, files) with Cleanup {

  override def getPortions: Seq[Identifiable] = this.filesIdentity

  override def getPartitioningRecord: Map[String, Int] = this.partitioningRecords
}
