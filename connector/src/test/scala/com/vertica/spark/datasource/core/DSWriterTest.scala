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

import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, TableName}
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType.MissingSchemaError
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

trait DummyWritePipe extends VerticaPipeInterface with VerticaPipeWriteInterface

class DSWriterTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  val config: DistributedFilesystemWriteConfig = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = new StructType(), targetTableSql = None, strlen = 1024)

  val uniqueId = "unique-id"

  private def checkResult(eith: Either[ConnectorError, Unit]): Unit= {
    eith match {
      case Left(err) => fail(err.msg)
      case Right(_) => ()
    }
  }

  it should "write several rows to a datablock" in {
    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock = DataBlock(List(InternalRow(v1,v2), InternalRow(v1,v3)))

    val pipe = mock[DummyWritePipe]
    (pipe.getDataBlockSize _).expects().returning(Right(2))
    (pipe.startPartitionWrite _).expects(uniqueId).returning(Right(()))
    (pipe.writeData _).expects(dataBlock).returning(Right(()))
    (pipe.endPartitionWrite _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getWritePipe _).expects(*).returning(pipe)

    val writer = new DSWriter(config, "unique-id", pipeFactory)

    checkResult(writer.openWrite())

    checkResult(writer.writeRow(dataBlock.data(0)))
    checkResult(writer.writeRow(dataBlock.data(1)))

    checkResult(writer.closeWrite())

  }

  it should "write extra rows on close" in {
    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock = DataBlock(List(InternalRow(v1,v2), InternalRow(v1,v3)))

    val pipe = mock[DummyWritePipe]
    (pipe.getDataBlockSize _).expects().returning(Right(3))
    (pipe.startPartitionWrite _).expects(uniqueId).returning(Right(()))
    (pipe.writeData _).expects(dataBlock).returning(Right(()))
    (pipe.endPartitionWrite _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getWritePipe _).expects(*).returning(pipe)

    val writer = new DSWriter(config, "unique-id", pipeFactory)

    checkResult(writer.openWrite())

    checkResult(writer.writeRow(dataBlock.data(0)))
    checkResult(writer.writeRow(dataBlock.data(1)))

    checkResult(writer.closeWrite())
  }

  it should "write rows to several datablocks" in {
    val v1: Int = 1
    val v2: Float = 2.0f
    val v3: Float = 3.0f
    val dataBlock1 = DataBlock(List(InternalRow(v1,v2), InternalRow(v1,v3)))
    val dataBlock2 = DataBlock(List(InternalRow(v1,v3), InternalRow(v1,v2)))

    val pipe = mock[DummyWritePipe]
    (pipe.getDataBlockSize _).expects().returning(Right(2))
    (pipe.startPartitionWrite _).expects(uniqueId).returning(Right(()))
    (pipe.writeData _).expects(dataBlock1).returning(Right(()))
    (pipe.writeData _).expects(dataBlock2).returning(Right(()))
    (pipe.endPartitionWrite _).expects().returning(Right(()))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getWritePipe _).expects(*).returning(pipe)

    val writer = new DSWriter(config, "unique-id", pipeFactory)

    checkResult(writer.openWrite())

    checkResult(writer.writeRow(dataBlock1.data(0)))
    checkResult(writer.writeRow(dataBlock1.data(1)))
    checkResult(writer.writeRow(dataBlock2.data(0)))
    checkResult(writer.writeRow(dataBlock2.data(1)))

    checkResult(writer.closeWrite())
  }

  it should "pass on errors from pipe" in {
    val pipe = mock[DummyWritePipe]
    (pipe.getDataBlockSize _).expects().returning(Left(ConnectorError(MissingSchemaError)))
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getWritePipe _).expects(*).returning(pipe)

    val writer = new DSWriter(config, "unique-id", pipeFactory)

    writer.openWrite() match {
      case Right(_) => fail
      case Left(err) => assert(err.err == MissingSchemaError)
    }
  }

  it should "call pipe commit on commit" in {
    val pipe = mock[DummyWritePipe]
    (pipe.commit _).expects().returning(Right())
    val pipeFactory = mock[VerticaPipeFactoryInterface]
    (pipeFactory.getWritePipe _).expects(*).returning(pipe)

    val writer = new DSWriter(config, "unique-id", pipeFactory)
    checkResult(writer.commitRows())
  }
}
