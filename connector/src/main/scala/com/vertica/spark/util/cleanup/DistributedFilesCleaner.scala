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
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.partitions.PartitionCleanup

/**
 * Class handles cleanup of exported files on file system. Intended to be used by each worker thread when finished.
 * */
class DistributedFilesCleaner(val config: DistributedFilesystemReadConfig, val cleanupUtils: CleanupUtilsInterface, val optionalFSLayer: Option[FileStoreLayerInterface] = None) {

  private val logger = LogProvider.getLogger(this)
  private val fileStoreLayer = optionalFSLayer.getOrElse(HadoopFileStoreLayer.make(config))
  private val fileStoreConfig = config.fileStoreConfig

  /**
   * The idea is to first writing to the filesystem, marking that a portion of a file has been read.
   * Then, we count if all portion of a file are present. Delete the file if so, else ignore.
   *
   * This is done for all partitions.
   *
   * @param partition The [[PartitionCleanup]] to be cleanup.
   * */
  def cleanupFiles(partition: PartitionCleanup): Unit = {
    logger.info("Removing files before closing read pipe.")

    for (fileIdx <- 0 to partition.getCleanUps.size) {
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
  }

  def getCleanupInfo(partition: PartitionCleanup, partitionIndex: Int): Option[FileCleanupInfo] = {
    logger.debug("Getting cleanup info for partition with idx " + partitionIndex)
    if (partitionIndex >= partition.getCleanUps.size) {
      logger.warn("Invalid fileIdx " + partitionIndex + ", can't perform cleanup.")
      None
    } else {
      val fileRange = partition.getCleanUps(partitionIndex)
      Some(FileCleanupInfo(fileRange.filename, fileRange.index, partition.getPartitioningRecord(fileRange.filename)))
    }
  }
}
