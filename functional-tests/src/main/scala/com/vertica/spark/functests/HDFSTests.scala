package com.vertica.spark.functests

import org.scalatest.BeforeAndAfterAll
import org.scalatest.BeforeAndAfterEach
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config.FileStoreConfig
import com.vertica.spark.datasource.fs.{HDFSLayer, HDFSReadLayer, HDFSReader}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
 * Tests basic functionality of the VerticaHDFSLayer
 *
 * TODO: Update this with write functionality once we figure out how to add parquet footer to written files
 * Should ensure that reading from HDFS works correctly, as well as other operations, such as creating/removing files/directories and listing files.
 */

class HDFSTests(val fsCfg: FileStoreConfig, val dirTestCfg: FileStoreConfig) extends AnyFlatSpec {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  private val df = spark.range(100).toDF("number")
  private val schema = df.schema

  it should "correctly read data from HDFS" in {
    val hdfsLayer = HDFSLayer(fsCfg)
    hdfsLayer.removeFile(fsCfg.address)
    df.write.parquet(fsCfg.address)

    val dataOrError = for {
      reader <- HDFSReader.openReadParquetFile(fsCfg, schema)
      fsReadLayer = HDFSReadLayer(fsCfg, reader)
      data <- fsReadLayer.readDataFromParquetFile(100)
      _ <- fsReadLayer.closeReadParquetFile()
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

  it should "create, list, and remove files from HDFS correctly" in {
    val fsLayer = HDFSLayer(dirTestCfg)
    fsLayer.removeDir(dirTestCfg.address)
    val unitOrError = for {
      _ <- fsLayer.createDir(dirTestCfg.address)
      _ <- fsLayer.createFile(dirTestCfg.address + "test.parquet")
      fileList1 <- fsLayer.getFileList(dirTestCfg.address)
      _ = fileList1.foreach(println)
      _ <- fsLayer.removeFile(dirTestCfg.address + "test.parquet")
      fileList2 <- fsLayer.getFileList(dirTestCfg.address)
      _ = fileList2.foreach(println)
      _ <- fsLayer.removeDir(dirTestCfg.address)
    } yield ()

    unitOrError match {
      case Right(_) => ()
      case Left(error) => fail(error.msg)
    }
  }
}
