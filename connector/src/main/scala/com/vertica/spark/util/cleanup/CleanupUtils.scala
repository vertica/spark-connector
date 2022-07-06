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

import cats.implicits._
import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.util.error.{CleanupError, ConnectorError, ParentDirMissingError}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.fs.Path

/**
 * Interface for cleanup util handling cleaning up files being accessed in a distributed fashion.
 *
 */
trait CleanupUtilsInterface {

  /**
   * Mark that this portion of the file is done with and cleanup if necessary.
   *
   * The cleanup step will only happen after this is called for each portion of the file.
   * @param fileStoreLayer Interface to interact with the filestore where files requiring cleaning are.
   * @param fileCleanupInfo Cleanup information for a portion of the file.
   */
  def checkAndCleanup(fileStoreLayer: FileStoreLayerInterface, fileCleanupInfo: FileCleanupInfo) : ConnectorResult[Unit]

  /**
   * Cleanup all files
   *
   * @param fileStoreLayer Interface to interact with the filestore where files requiring cleaning are.
   * @param path Path of directory
   */
  def cleanupAll(fileStoreLayer: FileStoreLayerInterface, path: String) : ConnectorResult[Unit]
}

class CleanupUtils extends CleanupUtilsInterface {
  private val logger = LogProvider.getLogger(classOf[CleanupUtils])
  private def recordFileName(filename: String, idx: Long) = filename + ".cleanup" + idx

  def cleanupAll(fileStoreLayer: FileStoreLayerInterface, path: String) : ConnectorResult[Unit] = {
    // Cleanup parent dir (unique id)
    val p = new Path(s"$path")
    val parent = p.getParent
    if(parent != null) {
      fileStoreLayer.removeDir(parent.toString)
      Right(())
    }
    else {
      Left(CleanupError(path))
    }
  }

  private def cleanupParentDirIfEmpty(fileStoreLayer: FileStoreLayerInterface, path: String): Either[ConnectorError, Unit]= {
    for {
      parentPath <- getParentHadoopPath(path)
      allFiles <- fileStoreLayer.getFileList(parentPath)
      parentPath2 <- getParentHadoopPath(parentPath)
      _ <- if(allFiles.isEmpty) fileStoreLayer.removeDir(parentPath2) else Right(())
    } yield ()
  }

  /**
   * Uses java-based hadoop path to retrieve the parent path. Checks for null safety here.
   */
  private def getParentHadoopPath(path: String): ConnectorResult[String] = {
    val p = new Path(s"$path")
    val parent = p.getParent
    if(parent != null) Right(parent.toString) else Left(ParentDirMissingError(path))
  }

  private def performCleanup(fileStoreLayer: FileStoreLayerInterface, fileCleanupInfo: FileCleanupInfo ): ConnectorResult[Unit] = {
    val filename = fileCleanupInfo.filename

    for {
      // Delete all portions
      _ <- (0 until fileCleanupInfo.fileRangeCount).map(idx => {
        logger.debug("Removing: " + recordFileName(filename, idx))
        fileStoreLayer.removeFile(recordFileName(filename, idx))}).toList.sequence

      // Delete the original file
      _ = logger.debug("Removing parquet: " + filename)
      _ <- fileStoreLayer.removeFile(filename)

      // Delete the directory if empty
      _ <- this.cleanupParentDirIfEmpty(fileStoreLayer, filename)
    } yield ()
  }

  override def checkAndCleanup(fileStoreLayer: FileStoreLayerInterface, fileCleanupInfo: FileCleanupInfo): ConnectorResult[Unit] = {
    val filename = fileCleanupInfo.filename
    logger.info("Doing partition cleanup of file: " + filename)

    for {
      // Create the file for this portion
      _ <- fileStoreLayer.createFile(recordFileName(filename, fileCleanupInfo.fileIdx))

      _ = logger.debug("File: " + filename + ", Checking file existance")
      _ = logger.debug("File: " + filename + ", File idx: " + fileCleanupInfo.fileIdx)
      _ = logger.debug("File: " + filename + ", File range count: " + fileCleanupInfo.fileRangeCount)

      // Check if all portions are written
      filesExist <- (0 until fileCleanupInfo.fileRangeCount).map(idx => {
           logger.debug("Checking existence: " + recordFileName(filename, idx))
           fileStoreLayer.fileExists(recordFileName(filename, idx))
        }).toList.sequence
      allExist <- Right(filesExist.forall(x => x))

      _ = logger.debug("File: " + filename + ", filesExist: " + filesExist.toString())
      _ = logger.debug("File: " + filename + ", All exist: " + allExist)

      _ <- if(allExist) {
        logger.debug("File: " + filename + ", Performing cleanup")
        performCleanup(fileStoreLayer, fileCleanupInfo)
      } else {
        Right(())
      }

    } yield ()
  }
}
