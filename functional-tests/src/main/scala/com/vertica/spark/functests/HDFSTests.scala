package com.vertica.spark.functests

import ch.qos.logback.classic.Level
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, VerticaMetadata}
import com.vertica.spark.datasource.core.ParquetFileRange
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

/**
 * Tests basic functionality of the VerticaHDFSLayer
 *
 * TODO: Update this with write functionality once we figure out how to add parquet footer to written files
 * Should ensure that reading from HDFS works correctly, as well as other operations, such as creating/removing files/directories and listing files.
 */

class HDFSTests(val fsCfgInit: DistributedFilesystemReadConfig, val dirTestCfgInit: DistributedFilesystemReadConfig) extends AnyFlatSpec with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  private val df = spark.range(100).toDF("number")
  private val schema = df.schema

  private val fsCfg = fsCfgInit.copy(metadata = Some(VerticaMetadata(schema)))
  private val dirTestCfg = dirTestCfgInit.copy(metadata = Some(VerticaMetadata(schema)))

  override def afterAll(): Unit = {
    spark.close()
  }

  it should "create, list, and remove files from HDFS correctly" in {
    val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), dirTestCfg)
    val path = dirTestCfg.fileStoreConfig.address
    fsLayer.removeDir(path)
    val unitOrError = for {
      _ <- fsLayer.createDir(path)
      _ <- fsLayer.createFile(path + "test.parquet")
      fileList1 <- fsLayer.getFileList(path)
      _ = fileList1.foreach(file => assert(file == path + "test.parquet"))
      _ <- fsLayer.removeFile(path + "test.parquet")
      fileList2 <- fsLayer.getFileList(path)
      _ = assert(fileList2.isEmpty)
      _ <- fsLayer.removeDir(path)
    } yield ()

    unitOrError match {
      case Right(_) => ()
      case Left(error) => fail(error.msg)
    }
  }

  it should "correctly read data from HDFS" in {
    val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), dirTestCfg)
    fsLayer.removeFile(fsCfg.fileStoreConfig.address)
    df.coalesce(1).write.format("parquet").mode("append").save(fsCfg.fileStoreConfig.address)
    //df.write.parquet(fsCfg.fileStoreConfig.address)

    val dataOrError = for {
      files <- fsLayer.getFileList(fsCfg.fileStoreConfig.address)
      _ <- fsLayer.openReadParquetFile(ParquetFileRange(files.filter(fname => fname.endsWith(".parquet")).head,0,0))
      data <- fsLayer.readDataFromParquetFile(100)
      _ <- fsLayer.closeReadParquetFile()
    } yield data

    dataOrError match {
      case Right(dataBlock) => dataBlock.data
          .map(row => row.get(0, LongType).asInstanceOf[Long])
          .sorted
          .zipWithIndex
          .foreach { case (rowValue, idx) => assert(rowValue == idx.toLong) }
      case Left(error) => fail(error.msg)
    }

  }

  it should "return an error when reading and the reader is uninitialized." in {
    val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), dirTestCfg)
    val dataOrError = fsLayer.readDataFromParquetFile(100)
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }

  it should "return an error when closing a read and the reader is uninitialized." in {
    val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), dirTestCfg)
    val dataOrError = fsLayer.closeReadParquetFile()
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }

  it should "return an error when reading and the schema has not been set in the config" in {
    val fsLayer = new HadoopFileStoreLayer(DistributedFilesystemWriteConfig(Level.ERROR), fsCfgInit)
    val dataOrError = fsLayer.readDataFromParquetFile(100)
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }
}
