package com.vertica.spark.datasource.core

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.datasource.v2.DummyReadPipe
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition


class DSReaderTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  val config: DistributedFilesystemReadConfig = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, partitionCount = None, metadata = None)

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

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
    (mockPipe.readData _).expects().returning(Left(ConnectorError(DoneReading)))
    (mockPipe.endPartitionRead _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*).returning(mockPipe)


    val reader = new DSReader(config, partition, pipeFactory)

    // Open
    reader.openRead() match {
      case Left(_) => fail
      case Right(()) => ()
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
    (mockPipe.readData _).expects().returning(Left(ConnectorError(DoneReading)))
    (mockPipe.endPartitionRead _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*).returning(mockPipe)


    val reader = new DSReader(config, partition, pipeFactory)

    // Open
    reader.openRead() match {
      case Left(_) => fail
      case Right(()) => ()
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
    (pipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val reader = new DSReader(config, partition, pipeFactory)

    // Open
    reader.openRead() match {
      case Left(err) => assert(err.err == InvalidPartition)
      case Right(()) => fail
    }
  }

  it should "Pass on errors from read pipe" in {
    val mockPipe = mock[DummyReadPipe]

    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val reader = new DSReader(config, partition, pipeFactory)
    (mockPipe.startPartitionRead _).expects(partition).returning(Left(ConnectorError(PartitioningError)))

    // Open
    reader.openRead() match {
      case Left(err) => assert(err.err == PartitioningError)
      case Right(()) => fail
    }
  }
}
