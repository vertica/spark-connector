package com.vertica.spark.functests

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig}
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.util.cleanup.{CleanupUtils, FileCleanupInfo}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class CleanupUtilTests(val cfg: DistributedFilesystemReadConfig) extends AnyFlatSpec with BeforeAndAfterAll {

  val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), cfg)
  val path = cfg.fileStoreConfig.address + "/CleanupTest"

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
}
