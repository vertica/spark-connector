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

import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.partitions.PartitionsCleanup
import com.vertica.spark.util.cleanup.DistributedFilesCleaner
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.{InputPartition, PartitionReader}

/**
 * Wraps a [[PartitionReader]], allowing us to intercept it's methods and add additional functionalities.
 * */
class PartitionReaderWrapper(val reader: PartitionReader[InternalRow],
                             val partitions: PartitionsCleanup,
                             val cleaner: DistributedFilesCleaner)
  extends PartitionReader[InternalRow] {

  private val logger = LogProvider.getLogger(classOf[PartitionReaderWrapper])

  override def next(): Boolean = reader.next()

  override def get(): InternalRow = reader.get()

  override def close(): Unit = {
    reader.close()
    cleaner.cleanupFiles(partitions)
    logger.info("Cleaning up")
  }
}
