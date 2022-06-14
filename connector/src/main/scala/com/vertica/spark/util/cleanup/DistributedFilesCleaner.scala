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

package com.vertica.spark.util.cleanup

import com.vertica.spark.config.{FileStoreConfig, LogProvider}
import com.vertica.spark.datasource.core.VerticaDistributedFilesystemPartition
import com.vertica.spark.datasource.core.partition.DistributedFilesystemPartition
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult

class DistributedFilesCleaner(val fileStoreConfig: FileStoreConfig,
                              val fileStoreLayer: FileStoreLayerInterface,
                              val cleanupUtils: CleanupUtilsInterface) {

  private val logger = LogProvider.getLogger(this)

  def start(partition: DistributedFilesystemPartition): ConnectorResult[Unit] = {
    for {
      _ <- cleanupFiles(partition)
      _ <- fileStoreLayer.closeReadParquetFile()
    } yield ()
  }

  def cleanupFiles(partition: DistributedFilesystemPartition): ConnectorResult[Unit] = {
    logger.info("Removing files before closing read pipe.")

    for (fileIdx <- 0 to partition.getFileRanges.size) {
      if (!fileStoreConfig.preventCleanup) {
        // Cleanup old file if required
        getCleanupInfo(partition, fileIdx) match {
          case Some(cleanupInfo) => cleanupUtils.checkAndCleanup(fileStoreLayer, cleanupInfo) match {
            case Left(err) => logger.warn("Ran into error when calling cleaning up. Treating as non-fatal. Err: " + err.getFullContext)
            case Right(_) => ()
          }
          case None => logger.warn("No cleanup info found.")
        }
      }
    }
    Right()
  }

  def getCleanupInfo(partition: DistributedFilesystemPartition, partitionIndex: Int): Option[FileCleanupInfo] = {
    logger.debug("Getting cleanup info for partition with idx " + partitionIndex)
    if (partitionIndex >= partition.getFileRanges.size) {
      logger.warn("Invalid fileIdx " + partitionIndex + ", can't perform cleanup.")
      None
    } else {
      val fileRange = partition.getFileRanges(partitionIndex)
      Some(FileCleanupInfo(fileRange.filename, fileRange.index, partition.getPartitioningRecord(fileRange.filename)))
    }
  }
}
