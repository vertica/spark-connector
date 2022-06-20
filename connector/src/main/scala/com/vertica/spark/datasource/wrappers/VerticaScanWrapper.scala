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

package com.vertica.spark.datasource.wrappers

import com.vertica.spark.datasource.partitions.file.{VerticaFilePartition, VerticaPartitionedFile}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReaderFactory, Scan}
import org.apache.spark.sql.execution.datasources.{FilePartition, PartitionedFile}
import org.apache.spark.sql.types.StructType

/**
 * Wraps a [[Scan]] so that it will create a [[PartitionReaderWrapperFactory]]
 *
 * planInputPartition() will also record partitioning information.
 * */
class VerticaScanWrapper(val scan: Scan) extends Scan with Batch {
  override def readSchema(): StructType = scan.readSchema()

  /**
   * Calls the wrapped scan to plan inputs. Then process them into [[VerticaFilePartition]] with partitioning info
   * */
  override def planInputPartitions(): Array[InputPartition] = {
    val partitioningRecords = scala.collection.mutable.Map[String, Int]()

    def recordPartitionedFile(file: PartitionedFile) = {
      val key = file.filePath
      val count = partitioningRecords.getOrElse(key, 0)
      partitioningRecords.put(key, count + 1)
      VerticaPartitionedFile(file, count)
    }

    scan.toBatch.planInputPartitions()
      .map(partition => partition.asInstanceOf[FilePartition])
      .map(filePartition =>
        filePartition.copy(files = filePartition.files.map(recordPartitionedFile)))
      .map(partition => new VerticaFilePartition(partition.index, partition.files, partitioningRecords.toMap))
  }

  override def createReaderFactory(): PartitionReaderFactory = new PartitionReaderWrapperFactory(scan.toBatch.createReaderFactory())

  override def toBatch: Batch = this
}
