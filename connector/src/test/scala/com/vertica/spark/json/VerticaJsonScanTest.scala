package com.vertica.spark.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.{DSConfigSetupInterface, PartitionInfo}
import com.vertica.spark.datasource.json.{JsonBatchFactory, VerticaJsonScan}
import com.vertica.spark.datasource.wrappers.VerticaScanWrapper
import com.vertica.spark.util.error.{ConnectorException, SchemaDiscoveryError}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory}
import org.apache.spark.sql.types.StructType
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.{Failure, Success, Try}

class VerticaJsonScanTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  private val readConfig = TestObjects.readConfig

  it should "read schema" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Right(StructType(List())))

    val scan = new VerticaJsonScan(readConfig, readSetup, new JsonBatchFactory)

    Try { scan.readSchema() } match {
      case Success(_) => ()
      case Failure(_) => fail
    }
  }

  it should "throw error on read schema fail" in {
    val readSetup = mock[DSConfigSetupInterface[ReadConfig]]
    (readSetup.getTableSchema _).expects(readConfig).returning(Left(SchemaDiscoveryError()))

    val scan = new VerticaJsonScan(readConfig, readSetup, new JsonBatchFactory)

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

    val jsonBatchFactory = mock[JsonBatchFactory]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonBatchFactory.build _).expects("path", *, *, *).returning(verticaScanWrapper)
    (verticaScanWrapper.asInstanceOf[Batch].planInputPartitions _).expects().returns(Array())

    Try { new VerticaJsonScan(readConfig, readSetup, jsonBatchFactory).planInputPartitions() } match {
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

    val batchFactory = mock[JsonBatchFactory]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (batchFactory.build _).expects("path", *, *, *).returning(verticaScanWrapper)
    val readerFactory = mock[PartitionReaderFactory]
    (verticaScanWrapper.asInstanceOf[Batch].createReaderFactory _).expects().returns(readerFactory)

    val scan = new VerticaJsonScan(readConfig, readSetup, batchFactory)
    Try { scan.createReaderFactory() } match {
      case Success(_) => ()
      case Failure(e) => fail(e)
    }
    spark.close()
  }
}
