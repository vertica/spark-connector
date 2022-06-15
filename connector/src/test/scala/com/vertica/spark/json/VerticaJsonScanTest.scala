package com.vertica.spark.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.{DSConfigSetupInterface, PartitionInfo}
import com.vertica.spark.datasource.v2.VerticaScan
import com.vertica.spark.util.error.{ConnectorException, SchemaDiscoveryError}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.catalog.{SupportsRead, SupportsWrite, Table, TableCapability}
import org.apache.spark.sql.connector.read.{Batch, InputPartition, PartitionReader, PartitionReaderFactory, Scan, ScanBuilder}
import org.apache.spark.sql.execution.datasources.json.JsonFileFormat
import org.apache.spark.sql.execution.datasources.v2.FileTable
import org.apache.spark.sql.execution.datasources.{FileFormat, FilePartition, PartitionedFile}
import org.apache.spark.sql.execution.datasources.v2.json.{JsonScanBuilder, JsonTable}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import java.util
import scala.collection.JavaConverters.setAsJavaSetConverter
import scala.util.{Failure, Success, Try}

class VerticaJsonScanTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  private val readConfig = TestObjects.readConfig

  class MockJsonTable(name: String,
                      sparkSession: SparkSession,
                      options: CaseInsensitiveStringMap,
                      paths: Seq[String],
                      userSpecifiedSchema: Option[StructType],
                      fallbackFileFormat: Class[_ <: FileFormat])
    extends JsonTable(name, sparkSession, options, paths, userSpecifiedSchema, fallbackFileFormat)

  it should "return table name" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val mockTable = new MockJsonTable("", spark, CaseInsensitiveStringMap.empty(), List(), None, classOf[JsonFileFormat])
    assert(new VerticaJsonTable(mockTable).name() == "VerticaJsonTable")
  }


  it should "return JsonTable capabilities" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val mockTable = new MockJsonTable("", spark, CaseInsensitiveStringMap.empty(), List(), None, classOf[JsonFileFormat])
    assert(new VerticaJsonTable(mockTable).capabilities() == mockTable.capabilities())
  }

  it should "read schema" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(StructType(List())))

    val scan = new VerticaJsonScan(readConfig, readSetup, new VerticaJsonScanSupport)

    Try { scan.readSchema() } match {
      case Success(_) => ()
      case Failure(_) => fail
    }
  }

  it should "throw error on read schema fail" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Left(SchemaDiscoveryError()))

    val scan = new VerticaJsonScan(readConfig, readSetup, new VerticaJsonScanSupport)

    Try { scan.readSchema() } match {
      case Success(_) => fail
      case Failure(err) => assert(err.asInstanceOf[ConnectorException].error.isInstanceOf[SchemaDiscoveryError])
    }
  }

  it should "perform initialization setup before returning partition info" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val schema = StructType(List())
    val partitionInfo = PartitionInfo(Array(), "path")
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Right(Some(partitionInfo)))
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(schema))

    val jsonSupport = mock[VerticaJsonScanSupport]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.getJsonScan _).expects("path", *, *).returning(verticaScanWrapper)
    (verticaScanWrapper.toBatch: () => Batch).expects().returns(verticaScanWrapper.asInstanceOf[Batch])
    (verticaScanWrapper.asInstanceOf[Batch].planInputPartitions _).expects().returns(Array())

    val scan = new VerticaJsonScan(readConfig, readSetup, jsonSupport)
    Try { scan.planInputPartitions() } match {
      case Success(_) => ()
      case Failure(e) => fail(e)
    }
    spark.close()
  }

  it should "perform initialization setup before creating a reader factory" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val schema = StructType(List())
    val partitionInfo = PartitionInfo(Array(), "path")
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.performInitialSetup _).expects(readConfig).returning(Right(Some(partitionInfo)))
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(schema))

    val jsonSupport = mock[VerticaJsonScanSupport]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.getJsonScan _).expects("path", *, *).returning(verticaScanWrapper)
    (verticaScanWrapper.toBatch: () => Batch).expects().returns(verticaScanWrapper.asInstanceOf[Batch])
    val readerFactory = mock[PartitionReaderFactory]
    (verticaScanWrapper.asInstanceOf[Batch].createReaderFactory _).expects().returns(readerFactory)

    val scan = new VerticaJsonScan(readConfig, readSetup, jsonSupport)
    Try { scan.createReaderFactory() } match {
      case Success(_) => ()
      case Failure(e) => fail(e)
    }
    spark.close()
  }

  it should "plan input partitions with partitioning records" in {
    val inputPartitions = Array(
      FilePartition(1, Array(
        PartitionedFile(InternalRow(), "path1", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path2", 0, 1, Array.empty)
      )),
      FilePartition(2, Array(
        PartitionedFile(InternalRow(), "path2", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path1", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path3", 0, 1, Array.empty)
      )),
      FilePartition(3, Array(
        PartitionedFile(InternalRow(), "path3", 0, 1, Array.empty),
        PartitionedFile(InternalRow(), "path4", 0, 1, Array.empty)
      )),
    )

    val scan = mock[Scan]
    val batch = mock[Batch]
    (scan.toBatch _).expects().returning(batch)
    (batch.planInputPartitions _).expects().returning(inputPartitions.asInstanceOf[Array[InputPartition]])

    val partitions = new VerticaScanWrapper(scan).planInputPartitions()
    assert(partitions.head.isInstanceOf[VerticaFilePartition])
    val verticaFilePartition = partitions.head.asInstanceOf[VerticaFilePartition]
    assert(verticaFilePartition.index == 1)
    assert(verticaFilePartition.files.head.isInstanceOf[VerticaPartitionedFile])
    val fileCounts = verticaFilePartition.partitioningRecords.keySet.toList.length
    assert(fileCounts == 4)
    assert(verticaFilePartition.partitioningRecords("path1") == 2)
    assert(verticaFilePartition.partitioningRecords("path2") == 2)
    assert(verticaFilePartition.partitioningRecords("path3") == 2)
    assert(verticaFilePartition.partitioningRecords("path4") == 1)
  }

  it should "build VerticaScanWrapper" in {
    val builder = mock[ScanBuilder]
    (builder.build _).expects().returning(mock[VerticaScanWrapper])
    assert(new VerticaScanWrapperBuilder(builder).build().isInstanceOf[VerticaScanWrapper])
  }

  it should "create wrapped partition reader factory" in {
    val scan = mock[Scan]
    val batch = mock[Batch]
    (scan.toBatch _).expects().returning(batch)
    (batch.createReaderFactory _).expects().returning(mock[PartitionReaderFactory])

    new VerticaScanWrapper(scan).createReaderFactory().isInstanceOf[PartitionReaderWrapperFactory]
  }

  it should "read schema from wrapped scan" in {
    val scan = mock[Scan]
    (scan.readSchema _).expects().returning(StructType(Nil))

    new VerticaScanWrapper(scan).readSchema()
  }

  it should "use wrapped reader to read" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.get _).expects().returning(mock[InternalRow])
    (reader.next _).expects()
    val partitions = mock[VerticaFilePartition]

    new PartitionReaderWrapper(reader, partitions).get()
    new PartitionReaderWrapper(reader, partitions).next()
  }

  it should "clean up on reader close" in {
    val reader = mock[PartitionReader[InternalRow]]
    (reader.close _).expects()
    val partitions = mock[VerticaFilePartition]
    // Todo: cleanup not yet supported.
    new PartitionReaderWrapper(reader, partitions).close()
  }

}
