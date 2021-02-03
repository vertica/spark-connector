package com.vertica.spark.functests

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig}
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.util.cleanup.{CleanupUtils, FileCleanupInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanupUtilTests(val cfg: FileStoreConfig) extends AnyFlatSpec with BeforeAndAfterAll {

  val fsLayer = new HadoopFileStoreLayer(cfg.logProvider, None)
  val path = cfg.address + "/CleanupTest"

  override def beforeAll() = {
    fsLayer.createDir(path)
  }

  override def afterAll() = {
    fsLayer.removeDir(path)
  }

  it should "Clean up a file" in {
    val filename = path + "/test.parquet"

    fsLayer.createFile(filename)

    CleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 0, 3))
    CleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 1, 3))
    CleanupUtils.checkAndCleanup(fsLayer, FileCleanupInfo(filename, 2, 3))

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
    fsLayer.createDir(uniqueDir)

    val childDir = uniqueDir + "/tablename"
    fsLayer.createDir(childDir)

    val filename = childDir + "/test.parquet"
    fsLayer.createFile(filename)

    assert(fsLayer.fileExists(uniqueDir).right.get)

    // Now test cleanup
    CleanupUtils.cleanupAll(fsLayer, childDir)

    assert(!fsLayer.fileExists(uniqueDir).right.get)
  }
}
