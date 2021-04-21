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

package com.vertica.spark.functests

import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, LogProvider}
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.util.cleanup.{CleanupUtils, FileCleanupInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanupUtilTests(val cfg: FileStoreConfig) extends AnyFlatSpec with BeforeAndAfterAll {

  val fsLayer = new HadoopFileStoreLayer(cfg, None)
  val path = cfg.address + "/CleanupTest"
  private val perms = "777"

  val cleanupUtils = new CleanupUtils

  override def beforeAll() = {
    fsLayer.createDir(path, perms)
  }

  override def afterAll() = {
    fsLayer.removeDir(path)
  }

  it should "Clean up a file" in {
    val filename = path + "/test.parquet"

    fsLayer.createFile(filename)

    cleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 0, 3))
    cleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 1, 3))
    cleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 2, 3))

    fsLayer.fileExists(filename) match {
      case Left(_) => fail
      case Right(exists) => assert(!exists)
    }
    fsLayer.fileExists(filename+".cleanup0") match {
      case Left(_) => fail
      case Right(exists) => assert(!exists)
    }
    fsLayer.fileExists(filename+".cleanup1") match {
      case Left(_) => fail
      case Right(exists) => assert(!exists)
    }
    fsLayer.fileExists(filename+".cleanup2") match {
      case Left(_) => fail
      case Right(exists) => assert(!exists)
    }
  }


  it should "Clean up parent unique directory" in {
    val uniqueDir = path + "/unique-dir-123"
    fsLayer.createDir(uniqueDir, perms)

    val childDir = uniqueDir + "/tablename"
    fsLayer.createDir(childDir, perms)

    val filename = childDir + "/test.parquet"
    fsLayer.createFile(filename)

    assert(fsLayer.fileExists(uniqueDir).right.get)

    // Now test cleanup
    cleanupUtils.cleanupAll(fsLayer, childDir)

    assert(!fsLayer.fileExists(uniqueDir).right.get)
  }
}
