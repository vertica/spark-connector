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

package com.vertica.spark.datasource.v2

import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.datasource._
import org.apache.spark.sql.types._
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.connector.catalog._
import org.scalamock.scalatest.MockFactory
import java.util

import cats.data.Validated.{Invalid, Valid}
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import ch.qos.logback.classic.Level
import com.vertica.spark.config.{DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, ReadConfig, TableName, ValidFilePermissions, WriteConfig}

import scala.collection.JavaConversions._
import com.vertica.spark.datasource.core._
import com.vertica.spark.util.error.{CloseReadError, ConfigBuilderError, ConnectorException, ErrorList, FileListEmptyPartitioningError, IntermediaryStoreReaderNotInitializedError, SchemaDiscoveryError, UserMissingError}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.write.LogicalWriteInfo
import org.apache.spark.sql.sources.{Filter, GreaterThan, LessThan}
import org.apache.spark.sql.util.CaseInsensitiveStringMap

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

trait DummyReadPipe extends VerticaPipeInterface with VerticaPipeReadInterface

class VerticaV2SourceTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory{

  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  val jOptions = new util.HashMap[String, String]()
  val options = new CaseInsensitiveStringMap(jOptions)

  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)
  val readConfig: DistributedFilesystemReadConfig = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, partitionCount = None, metadata = None, ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")))
  val writeConfig: DistributedFilesystemWriteConfig = DistributedFilesystemWriteConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, schema = new StructType(), targetTableSql = None, strlen = 1024, copyColumnList = None, sessionId = "id", failedRowPercentTolerance =  0.0f)

  val intSchema = new StructType(Array(StructField("col1", IntegerType)))

  val partition = VerticaDistributedFilesystemPartition(Seq(), None)

  it should "return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), jOptions)

    assert(table.isInstanceOf[VerticaTable])
  }

  it should "table returns scan builder" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.validateAndGetConfig _).expects(options.toMap).returning(Valid(readConfig))

    val table = new VerticaTable(options, readSetup)

    table.newScanBuilder(options) match {
      case _: VerticaScanBuilder => ()
      case _ => fail("Wrong scan builder built")
    }

    // Call a second time, returns the same scan builder (only one call to the above mock required
    table.newScanBuilder(options) match {
      case _: VerticaScanBuilder => ()
      case _ => fail("Wrong scan builder built")
    }
  }


  it should "throws error getting scan builder" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.validateAndGetConfig _).expects(options.toMap).returning(UserMissingError().invalidNec)

    val table = new VerticaTable(options, readSetup)

    Try {
      table.newScanBuilder(options)
    } match {
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error.asInstanceOf[ErrorList].errors.head match {
        case _: UserMissingError => ()
        case _ => fail
      }
    }
  }

  it should "table returns capabilities" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val table = new VerticaTable(options, readSetup)

    table.capabilities() == Set(TableCapability.BATCH_READ, TableCapability.BATCH_WRITE, TableCapability.OVERWRITE_BY_FILTER,
      TableCapability.TRUNCATE, TableCapability.ACCEPT_ANY_SCHEMA).asJava
  }

  it should "table returns schema" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.validateAndGetConfig _).expects(options.toMap).returning(Valid(readConfig)).twice()
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(intSchema))

    val table = new VerticaTable(options, readSetup)

    assert(table.schema() == intSchema)
  }

  it should "table returns empty table schema if no valid read of schema" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.validateAndGetConfig _).expects(options.toMap).returning(SchemaDiscoveryError(None).invalidNec)

    val table = new VerticaTable(options, readSetup)

    assert(table.schema() == new StructType())
  }

  it should "push filters on read" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val scanBuilder = new VerticaScanBuilder(readConfig, readSetup)
    val filters: Array[Filter] = Array(GreaterThan("col1", 5), LessThan("col2", 20))

    val nonPushed = scanBuilder.pushFilters(filters)
    assert(nonPushed.length == 0)

    scanBuilder.pushedFilters().sameElements(filters)
  }

  it should "prune columns on read" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val scanBuilder = new VerticaScanBuilder(readConfig, readSetup)

    scanBuilder.pruneColumns(intSchema)

    val scan = scanBuilder.build()

    assert(scan.asInstanceOf[VerticaScan].getConfig.getRequiredSchema == intSchema)
  }

  it should "scan return input partitions" in {
    val partitions: Array[InputPartition] = Array(partition)

    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Right(Some(PartitionInfo(partitions))))

    val scan = new VerticaScan(readConfig, readSetup)

    assert(scan.planInputPartitions().sameElements(partitions))
  }

  it should "scan return error on partitions" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Left(FileListEmptyPartitioningError()))

    val scan = new VerticaScan(readConfig, readSetup)

    Try {
      scan.planInputPartitions()
    } match {
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error match {
        case _ : FileListEmptyPartitioningError => ()
        case _ => fail(e)
      }
    }
  }

  it should "scan return reader factory" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val scan = new VerticaScan(readConfig, readSetup)

    assert(scan.createReaderFactory().isInstanceOf[VerticaReaderFactory])
  }

  it should "read from DSReader" in {
    val dsReader = mock[DSReaderInterface]
    (dsReader.openRead _).expects().returning(Right())
    (dsReader.readRow _).expects().returning(Right(Some(InternalRow(5))))
    (dsReader.readRow _).expects().returning(Right(Some(InternalRow(6))))
    (dsReader.readRow _).expects().returning(Right(Some(InternalRow(2))))
    (dsReader.readRow _).expects().returning(Right(Some(InternalRow(5))))
    (dsReader.readRow _).expects().returning(Right(None))
    (dsReader.closeRead _).expects().returning(Right())

    val batchReader = new VerticaBatchReader(readConfig, dsReader)

    var count = 0
    while(batchReader.next) {
      batchReader.get
      count += 1
    }

    assert(count == 4)

    batchReader.close()
  }


  it should "throw error in read" in {
    val dsReader = mock[DSReaderInterface]
    (dsReader.openRead _).expects().returning(Right())
    (dsReader.readRow _).expects().returning(Left(IntermediaryStoreReaderNotInitializedError()))

    val batchReader = new VerticaBatchReader(readConfig, dsReader)

    Try {
      batchReader.next
      batchReader.get
    } match {
      case Success(_) => fail
      case _ => ()
    }
  }

  it should "build vertica batch write" in {
    val info = mock[LogicalWriteInfo]
    (info.options _).expects().returning(options)
    val writeSetupInterface = mock[DSConfigSetupInterface[WriteConfig]]
    (writeSetupInterface.validateAndGetConfig _).expects(options.toMap).returning(writeConfig.validNec)

    val builder = new VerticaWriteBuilder(info, writeSetupInterface)

    assert(builder.buildForBatch().isInstanceOf[VerticaBatchWrite])
  }
}
