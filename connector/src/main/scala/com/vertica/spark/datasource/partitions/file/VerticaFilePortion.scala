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

import com.vertica.spark.datasource.partitions.FilePortion
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.PartitionedFile

/**
 * Extended from Spark's PartitionedFile with the purpose of holding extra partitioning information.
 *
 * @param partitionIndex The partition's id number out of all partitions created from a file.
 * */
class VerticaFilePortion(override val partitionValues: InternalRow,
                         override val filePath: String,
                         override val start: Long,
                         override val length: Long,
                         val partitionIndex: Int
                            )
  extends PartitionedFile(partitionValues, filePath, start, length) with FilePortion {
  override def filename: String = this.filePath

  override def end(): Long = this.start + this.length

  override def index(): Long = this.partitionIndex
}

object VerticaFilePortion {
  def apply(file: PartitionedFile, partitionIndex: Int): VerticaFilePortion =
    new VerticaFilePortion(file.partitionValues, file.filePath, file.start, file.length, partitionIndex)
}
