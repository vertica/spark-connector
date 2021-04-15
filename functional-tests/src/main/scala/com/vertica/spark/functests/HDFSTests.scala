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

import java.sql.{Connection, Timestamp}

import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.datasource.core.{DataBlock, ParquetFileRange}
import com.vertica.spark.datasource.fs.HadoopFileStoreLayer
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, VerticaJdbcLayer}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types._
import org.scalatest.BeforeAndAfterAll

/**
 * Tests basic functionality of the VerticaHDFSLayer
 *
 * TODO: Update this with write functionality once we figure out how to add parquet footer to written files
 * Should ensure that reading from HDFS works correctly, as well as other operations, such as creating/removing files/directories and listing files.
 */

class HDFSTests(val fsCfg: FileStoreConfig, val dirTestCfg: FileStoreConfig, val jdbcCfg: JDBCConfig) extends AnyFlatSpec with BeforeAndAfterAll {
  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  private val df = spark.range(100).toDF("number")
  private val schema = df.schema

  val fsLayer = new HadoopFileStoreLayer(dirTestCfg, dirTestCfg.logProvider, Some(schema))
  var jdbcLayer : JdbcLayerInterface = _

  override def beforeAll(): Unit = {
    jdbcLayer = new VerticaJdbcLayer(jdbcCfg)
  }

  override def afterAll(): Unit = {
    spark.close()
  }

  it should "create, list, and remove files from HDFS correctly" in {
    val path = dirTestCfg.address
    fsLayer.removeDir(path)
    val unitOrError = for {
      _ <- fsLayer.createDir(path, "777")
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
      case Left(error) => fail(error.getFullContext)
    }
  }

  /*
  it should "correctly read data from HDFS" in {
    fsLayer.removeFile(fsCfg.address)
    df.coalesce(1).write.format("parquet").mode("append").save(fsCfg.address)

    val dataOrError = for {
      files <- fsLayer.getFileList(fsCfg.address)
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
      case Left(error) => fail(error.getFullContext)
    }

    fsLayer.removeFile(fsCfg.address)
    fsLayer.createDir(fsCfg.address, "777")
  }
   */

  it should "return an error when reading and the reader is uninitialized." in {
    val dataOrError = fsLayer.readDataFromParquetFile(100)
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }

  it should "return an error when closing a read and the reader is uninitialized." in {
    val dataOrError = fsLayer.closeReadParquetFile()
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }

  it should "return an error when reading and the schema has not been set in the config" in {
    val dataOrError = fsLayer.readDataFromParquetFile(100)
    dataOrError match {
      case Right(_) => fail
      case Left(_) => ()
    }
  }

  it should "write then read a parquet file" in {
    val intSchema = new StructType(Array(StructField("a", IntegerType)))
    val path = fsCfg.address
    val filename = path + "testwriteread.parquet"

    fsLayer.openWriteParquetFile(filename)
    fsLayer.writeDataToParquetFile(DataBlock((0 until 100).map(a => InternalRow(a)).toList)) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
    fsLayer.closeWriteParquetFile()

    assert(fsLayer.fileExists(filename).right.getOrElse(false))

    fsLayer.openReadParquetFile(ParquetFileRange(filename, 0, 0, None))
    val dataOrError = fsLayer.readDataFromParquetFile(100)
    fsLayer.closeReadParquetFile()


    dataOrError match {
      case Right(dataBlock) =>
        assert(dataBlock.data.size == 100)
        dataBlock.data
        .map(row => row.get(0, IntegerType).asInstanceOf[Integer])
        .sorted
        .zipWithIndex
        .foreach { case (rowValue, idx) => assert(rowValue == idx.toInt) }
      case Left(error) => fail(error.getFullContext)
    }

  }


  it should "write then copy into vertica" in {
    val intSchema = new StructType(Array(StructField("a", IntegerType)))
    val fsLayer = new HadoopFileStoreLayer(dirTestCfg, dirTestCfg.logProvider, Some(intSchema))

    val path = fsCfg.address
    val filename = path + "testwriteload.parquet"

    fsLayer.openWriteParquetFile(filename)
    fsLayer.writeDataToParquetFile(DataBlock((0 until 100).map(a => InternalRow(a)).toList)) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
    fsLayer.closeWriteParquetFile()

    assert(fsLayer.fileExists(filename).right.getOrElse(false))

    val tablename = "testwriteload"
    // Create table
    val conn: Connection = TestUtils.getJDBCConnection(jdbcCfg)
    TestUtils.createTableBySQL(conn, tablename, "create table " + tablename + " (a int)")

    val copyStmt = s"COPY $tablename FROM '$filename' ON ANY NODE parquet"
    jdbcLayer.execute(copyStmt) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }

    // Verify proper copy of data
    var results = List[Int]()
    jdbcLayer.query("SELECT * FROM " + tablename + ";") match {
      case Right(rs) => {
        for(i <- 1 to 100) {
          assert(rs.next())
          results :+ rs.getInt(1)
        }
      }
      case Left(err) => {
        assert(false)
      }
    }

    results.sorted
      .zipWithIndex
      .foreach { case (rowValue, idx) => assert(rowValue == idx.toInt) }
  }


  it should "write a timestamp then copy into vertica" in {
    val timestampSchema = new StructType(Array(StructField("a", TimestampType)))
    val fsLayer = new HadoopFileStoreLayer(dirTestCfg, dirTestCfg.logProvider, Some(timestampSchema))
    val path = fsCfg.address
    val filename = path + "testwritetimestamp.parquet"

    val timestampInMicros = System.currentTimeMillis() * 1000

    fsLayer.openWriteParquetFile(filename)
    fsLayer.writeDataToParquetFile(DataBlock(List(InternalRow(timestampInMicros)))) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }
    fsLayer.closeWriteParquetFile()

    assert(fsLayer.fileExists(filename).right.getOrElse(false))

    val tablename = "testwritetimestamp"
    // Create table
    val conn: Connection = TestUtils.getJDBCConnection(jdbcCfg)
    TestUtils.createTableBySQL(conn, tablename, "create table " + tablename + " (a timestamp)")

    val copyStmt = s"COPY $tablename FROM '$filename' ON ANY NODE parquet"
    jdbcLayer.execute(copyStmt) match {
      case Left(err) => fail(err.getFullContext)
      case Right(_) => ()
    }

    jdbcLayer.query("SELECT count(*) FROM " + tablename + ";") match {
      case Right(rs) => {
        assert(rs.next())
        assert(rs.getInt(1) == 1)
      }
      case Left(err) => {
        fail
      }
    }
  }
}
