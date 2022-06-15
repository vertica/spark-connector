package com.vertica.spark.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.{DSConfigSetupInterface, PartitionInfo}
import com.vertica.spark.datasource.v2.VerticaScan
import com.vertica.spark.datasource.wrappers.json.VerticaJsonTableSupport
import com.vertica.spark.datasource.wrappers.{PartitionReaderWrapper, PartitionReaderWrapperFactory, VerticaScanWrapper, VerticaScanWrapperBuilder}
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

  it should "read schema" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(StructType(List())))

    val scan = new VerticaJsonScan(readConfig, readSetup, new VerticaJsonTableSupport)

    Try { scan.readSchema() } match {
      case Success(_) => ()
      case Failure(_) => fail
    }
  }

  it should "throw error on read schema fail" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Left(SchemaDiscoveryError()))

    val scan = new VerticaJsonScan(readConfig, readSetup, new VerticaJsonTableSupport)

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

    val jsonSupport = mock[VerticaJsonTableSupport]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.buildScan _).expects("path", *, *, *).returning(verticaScanWrapper)
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

    val jsonSupport = mock[VerticaJsonTableSupport]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.buildScan _).expects("path", *, *, *).returning(verticaScanWrapper)
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
}
