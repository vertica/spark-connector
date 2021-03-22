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

package com.vertica.spark.datasource.core

import java.sql.Connection
import java.util.Properties

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import com.vertica.spark.datasource.v2.DummyReadPipe
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition

import scala.util.{Success, Try}

class DSReaderTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  val config: DistributedFilesystemReadConfig = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, partitionCount = None, metadata = None, ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")))

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  private val mockConnectionCreator: (String, Properties) => Try[Connection] = (jdbcURI, prop) => Success(mock[Connection])

  val filename = "test.parquet"
  val partition: VerticaDistributedFilesystemPartition = VerticaDistributedFilesystemPartition(List(ParquetFileRange(filename, 0, 1)))



  it should "Read rows from data block" in {

    val v1: Int = 1
    val v2: Float = 2.0f
    val row = InternalRow(v1, v2)
    val data = DataBlock(List(row, row))

    val mockPipe = mock[DummyReadPipe]
    (mockPipe.startPartitionRead _).expects(partition).returning(Right(()))
    (mockPipe.readData _).expects().returning(Right(data))
    (mockPipe.readData _).expects().returning(Left(DoneReading()))
    (mockPipe.endPartitionRead _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*, *).returning(Right(mockPipe))

    val reader = (for {
      pipe <- pipeFactory.getReadPipe(config, mockConnectionCreator)
      reader <- DSReader.makeDSReader(pipe, config, partition)
    } yield reader) match {
      case Left(err) => fail(err.getFullContext)
      case Right(reader) => reader
    }

    // 1st row
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => fail
          case Some(r) =>
            assert(r.getInt(0) == v1)
        }
    }

    // 2nd row
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => fail
          case Some(r) =>
            assert(r.getFloat(1) == v2)
        }
    }

    // Nothing more to read
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => ()
          case Some(_) => fail
        }
    }

    // Close
    reader.closeRead() match {
      case Left(_) => fail
      case Right(()) => ()
    }
  }

  it should "Read rows from several data blocks" in {
    val v1: Int = 1
    val v2: Float = 2.0f
    val row = InternalRow(v1, v2)
    val data = DataBlock(List(row))

    val mockPipe = mock[DummyReadPipe]
    (mockPipe.startPartitionRead _).expects(partition).returning(Right(()))
    (mockPipe.readData _).expects().returning(Right(data))
    (mockPipe.readData _).expects().returning(Right(data))
    (mockPipe.readData _).expects().returning(Left(DoneReading()))
    (mockPipe.endPartitionRead _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*, *).returning(Right(mockPipe))

    val reader = (for {
      pipe <- pipeFactory.getReadPipe(config, mockConnectionCreator)
      reader <- DSReader.makeDSReader(pipe, config, partition)
    } yield reader) match {
      case Left(err) => fail(err.getFullContext)
      case Right(reader) => reader
    }

    // 1st row
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => fail
          case Some(r) =>
            assert(r.getInt(0) == v1)
        }
    }

    // 2nd row
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => fail
          case Some(r) =>
            assert(r.getFloat(1) == v2)
        }
    }

    // Nothing more to read
    reader.readRow() match {
      case Left(_) => fail
      case Right(row) =>
        row match {
          case None => ()
          case Some(_) => fail
        }
    }

    // Close
    reader.closeRead() match {
      case Left(_) => fail
      case Right(()) => ()
    }
  }

  it should "Error out on unexpected partition type" in {
    val partition = mock[InputPartition]

    val mockPipe = mock[DummyReadPipe]
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*, *).returning(Right(mockPipe))

    (for {
      pipe <- pipeFactory.getReadPipe(config, mockConnectionCreator)
      reader <- DSReader.makeDSReader(pipe, config, partition)
    } yield reader) match {
      case Left(err) => assert(err.getError == InvalidPartition())
      case Right(_) => fail
    }
  }

  it should "Pass on errors from read pipe" in {
    val mockPipe = mock[DummyReadPipe]

    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*, *).returning(Right(mockPipe))
    (mockPipe.startPartitionRead _).expects(partition).returning(Left(InitialSetupPartitioningError()))

    (for {
      pipe <- pipeFactory.getReadPipe(config, mockConnectionCreator)
      reader <- DSReader.makeDSReader(pipe, config, partition)
    } yield reader) match {
      case Left(err) => assert(err.getError == InitialSetupPartitioningError())
      case Right(_) => fail
    }

  }
}
