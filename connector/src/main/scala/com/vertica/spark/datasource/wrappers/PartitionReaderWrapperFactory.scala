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

import com.vertica.spark.config.{DistributedFilesystemReadConfig, ReadConfig}
import com.vertica.spark.datasource.partitions.DistributedFilesystemPartition
import com.vertica.spark.util.cleanup.{CleanupUtils, DistributedFilesCleaner}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader, PartitionReaderFactory}

/**
 * Wraps a [[PartitionReaderFactory]] so it will create a [[PartitionReaderWrapper]]
 *
 * planInputPartition() will also record partitioning information.
 * */
class PartitionReaderWrapperFactory(val readerFactory: PartitionReaderFactory, val config: ReadConfig)
  extends PartitionReaderFactory {

  override def createReader(inputPartition: InputPartition): PartitionReader[InternalRow] = {
    config match {
      case readConfig: DistributedFilesystemReadConfig =>
        val reader = readerFactory.createReader(inputPartition)
        val partition = inputPartition.asInstanceOf[DistributedFilesystemPartition]
        val cleaner = new DistributedFilesCleaner(readConfig, new CleanupUtils)
        new PartitionReaderWrapper(reader, partition, cleaner)
    }
  }
}
