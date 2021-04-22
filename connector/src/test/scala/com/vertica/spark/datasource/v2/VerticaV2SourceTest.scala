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

import cats.data.Validated.Valid
import cats.implicits.catsSyntaxValidatedIdBinCompat0
import com.vertica.spark.config.{BasicJdbcAuth, DistributedFilesystemReadConfig, DistributedFilesystemWriteConfig, FileStoreConfig, JDBCConfig, ReadConfig, TableName, ValidFilePermissions, WriteConfig}

import scala.collection.JavaConversions._
import com.vertica.spark.datasource.core._
import com.vertica.spark.util.error.{ConnectorException, ErrorList, FileListEmptyPartitioningError, InitialSetupPartitioningError, IntermediaryStoreReaderNotInitializedError, IntermediaryStoreWriterNotInitializedError, SchemaDiscoveryError, UserMissingError}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.read.InputPartition
import org.apache.spark.sql.connector.write.{LogicalWriteInfo, PhysicalWriteInfo}
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
  jOptions.put("table", "t1")
  val options = new CaseInsensitiveStringMap(jOptions)

  val tablename: TableName = TableName("testtable", None)
  val jdbcConfig: JDBCConfig = JDBCConfig("1.1.1.1", 1234, "test", BasicJdbcAuth("test", "test"))
  val fileStoreConfig: FileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test")
  val readConfig: DistributedFilesystemReadConfig = DistributedFilesystemReadConfig(jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tableSource = tablename, partitionCount = None, metadata = None, ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")), 64, 512)
  val writeConfig: DistributedFilesystemWriteConfig = DistributedFilesystemWriteConfig(
    jdbcConfig = jdbcConfig,
    fileStoreConfig = fileStoreConfig,
    tablename = tablename,
    schema = new StructType(),
    targetTableSql = None,
    strlen = 1024,
    copyColumnList = None,
    sessionId = "id",
    failedRowPercentTolerance =  0.0f,
    filePermissions = ValidFilePermissions("777").getOrElse(throw new Exception("File perm error")))

  val intSchema = new StructType(Array(StructField("col1", IntegerType)))

  private val partition = VerticaDistributedFilesystemPartition(Seq(), None)

  it should "get no catalog options" in {
    VerticaDatasourceV2Catalog.getOptions match {
      case Some(_) => fail
      case None => ()
    }
  }

  it should "return a Vertica Table" in {
    val source = new VerticaSource()
    val table = source.getTable(new StructType(), Array[Transform](), jOptions)

    assert(table.isInstanceOf[VerticaTable])
  }

  it should "extract identifier from options" in {
    val source = new VerticaSource()

    val ident = source.extractIdentifier(options)

    assert(ident.name() == "t1")
  }

  it should "fail to extract catalog name without spark session" in {
    val source = new VerticaSource()

    Try {
      source.extractCatalog(options)
    } match {
      case Success(_) => fail
      case Failure(_) => ()
    }
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

  it should "throw error on input partitions" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Left(FileListEmptyPartitioningError()))

    val scan = new VerticaScan(readConfig, readSetup)

    Try { scan.planInputPartitions() } match {
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error.isInstanceOf[FileListEmptyPartitioningError]
    }
  }

  it should "throw error on bad schema read" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Left(SchemaDiscoveryError(None)))

    val scan = new VerticaScan(readConfig, readSetup)

    Try { scan.readSchema() } match {
      case Success(_) => fail
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error.isInstanceOf[SchemaDiscoveryError]
    }
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

  it should "throw error on empty partition info" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Right(None))

    val scan = new VerticaScan(readConfig, readSetup)

    Try { scan.planInputPartitions() } match {
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error.isInstanceOf[InitialSetupPartitioningError]
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
      case Failure(_) => ()
    }
  }

  it should "throw error in open read" in {
    val dsReader = mock[DSReaderInterface]
    (dsReader.openRead _).expects().returning(Left(IntermediaryStoreReaderNotInitializedError()))

    Try {
      new VerticaBatchReader(readConfig, dsReader)
    } match {
      case Success(_) => fail
      case Failure(_) => ()
    }
  }

  it should "throw error in close read" in {
    val dsReader = mock[DSReaderInterface]
    (dsReader.openRead _).expects().returning(Right())
    (dsReader.closeRead _).expects().returning(Left(IntermediaryStoreReaderNotInitializedError()))

    val batchReader = new VerticaBatchReader(readConfig, dsReader)

    Try {
      batchReader.close()
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
    (writeSetupInterface.performInitialSetup _).expects(writeConfig).returning(Right(None))

    val builder = new VerticaWriteBuilder(info, writeSetupInterface)

    assert(builder.buildForBatch().isInstanceOf[VerticaBatchWrite])
  }

  it should "throw error on creating write builder" in {
    val info = mock[LogicalWriteInfo]
    (info.options _).expects().returning(options)

    val writeSetupInterface = mock[DSConfigSetupInterface[WriteConfig]]
    (writeSetupInterface.validateAndGetConfig _).expects(options.toMap).returning(UserMissingError().invalidNec)

    Try { new VerticaWriteBuilder(info, writeSetupInterface) } match {
      case Success(_) => fail
      case Failure(_) => ()
    }
  }

  it should "throw error on creating batch write" in {
    val info = mock[LogicalWriteInfo]

    val writeSetupInterface = mock[DSConfigSetupInterface[WriteConfig]]
    (writeSetupInterface.performInitialSetup _).expects(writeConfig).returning(Left(UserMissingError()))

    Try { new VerticaBatchWrite(writeConfig, writeSetupInterface) } match {
      case Success(_) => fail
      case Failure(_) => ()
    }
  }

  it should "batch write creates writer factory" in {
    val writeSetupInterface = mock[DSConfigSetupInterface[WriteConfig]]
    (writeSetupInterface.performInitialSetup _).expects(writeConfig).returning(Right(None))
    val physicalInfo = mock[PhysicalWriteInfo]

    val batchWrite = new VerticaBatchWrite(writeConfig, writeSetupInterface)

    assert(batchWrite.createBatchWriterFactory(physicalInfo).isInstanceOf[VerticaWriterFactory])
  }

  it should "throw error on abort" in {
    val writeSetupInterface = mock[DSConfigSetupInterface[WriteConfig]]
    (writeSetupInterface.performInitialSetup _).expects(writeConfig).returning(Right(None))

    val batchWrite = new VerticaBatchWrite(writeConfig, writeSetupInterface)

    Try {
      batchWrite.abort(Array())
    } match {
      case Success(_) => fail
      case Failure(e) => e.asInstanceOf[ConnectorException].error.isInstanceOf[JobAbortedError]
    }
  }

  it should "batch writer writes" in {
    val dsWriter = mock[DSWriterInterface]
    (dsWriter.openWrite _).expects().returning(Right())
    (dsWriter.writeRow _).expects(InternalRow(5)).returning(Right())
    (dsWriter.writeRow _).expects(InternalRow(6)).returning(Right())
    (dsWriter.writeRow _).expects(InternalRow(7)).returning(Right())
    (dsWriter.writeRow _).expects(InternalRow(8)).returning(Right())
    (dsWriter.closeWrite _).expects().returning(Right())

    val batchWriter = new VerticaBatchWriter(writeConfig, dsWriter)
    batchWriter.write(InternalRow(5))
    batchWriter.write(InternalRow(6))
    batchWriter.write(InternalRow(7))
    batchWriter.write(InternalRow(8))

    assert(batchWriter.commit() == WriteSucceeded)
  }

  it should "throws error on write" in {
    val dsWriter = mock[DSWriterInterface]
    (dsWriter.openWrite _).expects().returning(Right())
    (dsWriter.writeRow _).expects(InternalRow(5)).returning(Left(IntermediaryStoreWriterNotInitializedError()))

    val batchWriter = new VerticaBatchWriter(writeConfig, dsWriter)

    Try {
      batchWriter.write(InternalRow(5))
    } match {
      case Success(_) => fail
      case Failure(e) => ()
    }
  }

  it should "throws error on open write" in {
    val dsWriter = mock[DSWriterInterface]
    (dsWriter.openWrite _).expects().returning(Left(IntermediaryStoreWriterNotInitializedError()))

    Try {
      new VerticaBatchWriter(writeConfig, dsWriter)
    } match {
      case Success(_) => fail
      case Failure(e) => ()
    }
  }

  it should "throws error on writer commit" in {
    val dsWriter = mock[DSWriterInterface]
    (dsWriter.openWrite _).expects().returning(Right())
    (dsWriter.closeWrite _).expects().returning(Left(IntermediaryStoreWriterNotInitializedError()))

    val batchWriter = new VerticaBatchWriter(writeConfig, dsWriter)

    Try {
      batchWriter.commit()
    } match {
      case Success(_) => fail
      case Failure(e) => ()
    }
  }

  /**
   * More relevant error thrown earlier in abort case
   */
  it should "throws no error on abort" in {
    val dsWriter = mock[DSWriterInterface]
    (dsWriter.openWrite _).expects().returning(Right())
    (dsWriter.closeWrite _).expects().returning(Left(IntermediaryStoreWriterNotInitializedError()))

    val batchWriter = new VerticaBatchWriter(writeConfig, dsWriter)

    Try {
      batchWriter.abort()
      batchWriter.close()
    } match {
      case Success(_) => ()
      case Failure(e) => fail(e)
    }
  }

  it should "catalog tests if table exists" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.validateAndGetConfig _).expects(options.toMap).returning(Valid(readConfig)).twice()
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(intSchema))

    val catalog = new VerticaDatasourceV2Catalog()
    catalog.readSetupInterface = readSetup

    catalog.initialize("VerticaTable", options)

    // Implementation currently based on config, not identifier
    assert(catalog.tableExists(mock[Identifier]))
  }

  it should "catalog loads table on load or create" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val catalog = new VerticaDatasourceV2Catalog()
    catalog.readSetupInterface = readSetup

    catalog.initialize("VerticaTable", options)

    assert(catalog.loadTable(mock[Identifier]).isInstanceOf[VerticaTable])
    assert(catalog.createTable(mock[Identifier], new StructType(), Array(), jOptions).isInstanceOf[VerticaTable])
  }

  it should "catalog does not support other operations" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]

    val catalog = new VerticaDatasourceV2Catalog()
    catalog.readSetupInterface = readSetup

    Try { catalog.alterTable(mock[Identifier], mock[TableChange]) }
    match {
      case Success(_) => fail
      case Failure(e) => assert(e.isInstanceOf[NoCatalogException])
    }

    Try { catalog.renameTable(mock[Identifier], mock[Identifier]) }
    match {
      case Success(_) => fail
      case Failure(e) => assert(e.isInstanceOf[NoCatalogException])
    }

    Try { catalog.dropTable(mock[Identifier]) }
    match {
      case Success(_) => fail
      case Failure(e) => assert(e.isInstanceOf[NoCatalogException])
    }

    Try { catalog.listTables(Array()) }
    match {
      case Success(_) => fail
      case Failure(e) => assert(e.isInstanceOf[NoCatalogException])
    }
  }

  it should "get/set catalog options" in {
    val operationM = Map("thing" -> "thing")
    val opOpts = new CaseInsensitiveStringMap(operationM)

    VerticaDatasourceV2Catalog.setOptions(opOpts)

    assert(VerticaDatasourceV2Catalog.getOptions.get.containsKey("thing"))
  }
}
