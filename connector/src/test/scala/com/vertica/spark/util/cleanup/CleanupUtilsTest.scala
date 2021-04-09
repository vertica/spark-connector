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

import com.vertica.spark.config.LogProvider
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.util.error.{CleanupError, CreateFileError, RemoveFileError}
import org.apache.hadoop.fs.Path
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanupUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  val cleanupUtils = new CleanupUtils

  it should "Clean up a file with a single part" in {
    val filename = "path/path/file.parquet"

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createFile _).expects(filename+".cleanup0").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup0").returning(Right(()))
    (fileStoreLayer.removeFile _).expects(filename).returning(Right(()))
    (fileStoreLayer.getFileList _).expects("path/path").returning(Right(Seq[String]()))
    (fileStoreLayer.removeDir _).expects("path").returning(Right(()))

    val fileCleanupInfo = FileCleanupInfo(filename, 0, 1)

    cleanupUtils.checkAndCleanup(fileStoreLayer, fileCleanupInfo)
  }

  it should "Don't perform any cleanup if other parts aren't complete" in {
    val filename = "file.parquet"

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createFile _).expects(filename+".cleanup0").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(false))
    (fileStoreLayer.removeFile _).expects(*).never

    val fileCleanupInfo = FileCleanupInfo(filename, 0, 2)

    cleanupUtils.checkAndCleanup(fileStoreLayer, fileCleanupInfo)
  }

  it should "Clean up a file with a multiple parts" in {
    val filename = "path/path/file.parquet"

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createFile _).expects(filename+".cleanup0").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(false))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup2").returning(Right(false))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 0, 3))

    (fileStoreLayer.createFile _).expects(filename+".cleanup1").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup2").returning(Right(false))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 1, 3))

    (fileStoreLayer.createFile _).expects(filename+".cleanup2").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup2").returning(Right(true))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup0").returning(Right(()))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup1").returning(Right(()))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup2").returning(Right(()))
    (fileStoreLayer.removeFile _).expects(filename).returning(Right(()))
    (fileStoreLayer.getFileList _).expects("path/path").returning(Right(Seq[String]()))
    (fileStoreLayer.removeDir _).expects("path").returning(Right(()))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 2, 3))
  }

  it should "Pass on errors from the file store layer" in {
    val filename = "file.parquet"

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.createFile _).expects(filename+".cleanup0").returning(Left(CreateFileError(new Path(filename), new Exception())))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 0, 3)) match {
      case Right(_) => ()
      case Left(err) => assert(err.getError match {
        case CreateFileError(_, _) => true
        case _ => false
      })
    }

    (fileStoreLayer.createFile _).expects(filename+".cleanup1").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Left(CreateFileError(new Path(filename), new Exception())))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup2").returning(Right(true))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 1, 3)) match {
      case Right(_) => ()
      case Left(err) => assert(err.getError match {
        case CreateFileError(_, _) => true
        case _ => false
      })
    }

    (fileStoreLayer.createFile _).expects(filename+".cleanup2").returning(Right(()))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup0").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup1").returning(Right(true))
    (fileStoreLayer.fileExists _).expects(filename+".cleanup2").returning(Right(true))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup0").returning(Left(RemoveFileError(new Path(filename), new Exception())))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup1").returning(Left(RemoveFileError(new Path(filename), new Exception())))
    (fileStoreLayer.removeFile _).expects(filename+".cleanup2").returning(Left(RemoveFileError(new Path(filename), new Exception())))

    cleanupUtils.checkAndCleanup(fileStoreLayer, FileCleanupInfo(filename, 2, 3)) match {
      case Right(_) => ()
      case Left(err) => assert(err.getError match {
        case RemoveFileError(_, _) => true
        case _ => false
      })
    }
  }

  it should "Clean up all" in {
    val dirname = "path/path"

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects("path")

    cleanupUtils.cleanupAll(fileStoreLayer, dirname) match {
      case Left(e) => fail("Failure: " + e)
      case Right(_) => ()
    }
  }

  it should "Return error if java path returns null on cleanup all" in {
    val dirname = "/"

    val fileStoreLayer = mock[FileStoreLayerInterface]

    cleanupUtils.cleanupAll(fileStoreLayer, dirname) match {
      case Right(_) => fail
      case Left(e) => e.isInstanceOf[CleanupError]
    }
  }
}
