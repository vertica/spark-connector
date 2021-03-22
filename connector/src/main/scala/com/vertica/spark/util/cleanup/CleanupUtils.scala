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

import cats.implicits.toTraverseOps
import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.util.error.{CleanupError, ConnectorError, FileSystemError}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import org.apache.hadoop.fs.Path

/**
 * Structure containing cleanup information for a given portion of a file.
 *
 * @param filename The file to check for cleanup.
 * @param fileIdx Which portion of the file is done being read.
 * @param fileRangeCount How many portions of the file exist.
 */
final case class FileCleanupInfo(filename: String, fileIdx: Int, fileRangeCount: Int)

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

class CleanupUtils(logProvider: LogProvider) extends CleanupUtilsInterface {
  private val logger = logProvider.getLogger(classOf[CleanupUtils])
  private def recordFileName(filename: String, idx: Int) = filename + ".cleanup" + idx

  def cleanupAll(fileStoreLayer: FileStoreLayerInterface, path: String) : ConnectorResult[Unit] = {
    // Cleanup parent dir (unique id)
    /*
    val p = new Path(s"$path")
    val parent = p.getParent
    if(parent != null) {
      fileStoreLayer.removeDir(parent.toString)
      Right(())
    }
    else {
      Left(CleanupError(path))
    }
     */
    Right(())
  }

  private def cleanupDirIfEmpty(fileStoreLayer: FileStoreLayerInterface, path: String): Either[ConnectorError, Unit]= {
    for {
      allFiles <- fileStoreLayer.getFileList(path)
      _ <- if(allFiles.isEmpty) fileStoreLayer.removeDir(path) else Right(())
    } yield ()
  }

  /**
   * Uses java-based hadoop path to retrieve the parent path. Checks for null safety here.
   */
  private def getParentHadoopPath(path: String): ConnectorResult[String] = {
    val p = new Path(s"$path")
    val parent = p.getParent
    if(parent != null) Right(parent.toString) else Left(FileSystemError().context("Could not retrieve parent path of file: " + path))
  }

  private def performCleanup(fileStoreLayer: FileStoreLayerInterface, fileCleanupInfo: FileCleanupInfo ): ConnectorResult[Unit] = {
    val filename = fileCleanupInfo.filename

    for {
      // Delete all portions
      _ <- (0 until fileCleanupInfo.fileRangeCount).map(idx =>
          fileStoreLayer.removeFile(recordFileName(filename, idx))).toList.sequence

      // Delete the original file
      _ <- fileStoreLayer.removeFile(filename)

      // Delete the directory if empty
      parentPath <- getParentHadoopPath(filename)
      _ <- this.cleanupDirIfEmpty(fileStoreLayer, parentPath)
    } yield ()
  }

  override def checkAndCleanup(fileStoreLayer: FileStoreLayerInterface, fileCleanupInfo: FileCleanupInfo): ConnectorResult[Unit] = {
    val filename = fileCleanupInfo.filename
    logger.info("Doing partition cleanup of file: " + filename)

    for {
      // Create the file for this portion
      _ <- fileStoreLayer.createFile(recordFileName(filename, fileCleanupInfo.fileIdx))

      _ = java.lang.Thread.sleep(2000L)

      // Check if all portions are written
      filesExist <- (0 until fileCleanupInfo.fileRangeCount).map(idx =>
          fileStoreLayer.fileExists(recordFileName(filename, idx))
        ).toList.sequence
      allExist <- Right(filesExist.forall(identity))

      _ <- if(allExist) performCleanup(fileStoreLayer, fileCleanupInfo) else Right(())

    } yield ()
  }
}
