package com.vertica.spark.datasource.json

import com.vertica.spark.common.TestObjects
import com.vertica.spark.config.ReadConfig
import com.vertica.spark.config.VerticaReadMetadata
import com.vertica.spark.datasource.core._
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.wrappers.VerticaScanWrapper
import com.vertica.spark.util.error.{ConnectorException, MetadataDiscoveryError}
import com.vertica.spark.util.version.VerticaVersionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.read.{Batch, PartitionReaderFactory}
import org.apache.spark.sql.types._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import scala.util.{Failure, Success, Try}

class VerticaJsonScanTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  private val jsonReadConfig = TestObjects.readConfig.copy(useJson = true)
  val intSchema = new StructType(Array(StructField("col1", IntegerType)))
  val intMeta = new VerticaReadMetadata(intSchema, VerticaVersionUtils.VERTICA_DEFAULT)


  it should "read schema" in {
    val readSetup = mock[DSReadConfigSetup]     

    (readSetup.getTableMetadata _).expects(jsonReadConfig).returning(Right(intMeta))

    val scan = new VerticaJsonScan(jsonReadConfig, readSetup.asInstanceOf[DSConfigSetupInterface[ReadConfig] with TableMetaInterface[ReadConfig]], new JsonBatchFactory, mock[FileStoreLayerInterface])

    Try { scan.readSchema() } match {
      case Success(_) => ()
      case Failure(err) => fail(err)
    }
  }

  it should "throw error on read schema fail" in {
    val readSetup = mock[DSReadConfigSetup]
    (readSetup.getTableMetadata _).expects(jsonReadConfig).returning(Left(MetadataDiscoveryError()))

    val scan = new VerticaJsonScan(jsonReadConfig, readSetup.asInstanceOf[DSConfigSetupInterface[ReadConfig] with TableMetaInterface[ReadConfig]], new JsonBatchFactory, mock[FileStoreLayerInterface])

    Try { scan.readSchema() } match {
      case Success(_) => fail
      case Failure(err) => assert(err.asInstanceOf[ConnectorException].error.isInstanceOf[MetadataDiscoveryError])
    }
  }

  it should "perform initialization setup before returning partition info" in {
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test")
      .getOrCreate()

    val schema = StructType(List())
    val partitionInfo = PartitionInfo(Array(), "path")
    val readSetup = mock[DSReadConfigSetup]
    (readSetup.performInitialSetup _).expects(jsonReadConfig).returning(Right(Some(partitionInfo)))
    (readSetup.getTableMetadata _).expects(jsonReadConfig).returning(Right(intMeta))

    val jsonSupport = mock[JsonBatchFactory]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.build _).expects("path", *, *, *).returning(verticaScanWrapper)
    (verticaScanWrapper.asInstanceOf[Batch].planInputPartitions _).expects().returns(Array())
    val fsLayer = mock[FileStoreLayerInterface]
    (fsLayer.getFileList _).expects("path").returning(Right(Seq("file1.json", "file2.json")))

    val scan = new VerticaJsonScan(jsonReadConfig, readSetup.asInstanceOf[DSConfigSetupInterface[ReadConfig] with TableMetaInterface[ReadConfig]], jsonSupport, fsLayer)
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
    val readSetup = mock[DSReadConfigSetup]
    (readSetup.performInitialSetup _).expects(jsonReadConfig).returning(Right(Some(partitionInfo)))
    (readSetup.getTableMetadata _).expects(jsonReadConfig).returning(Right(intMeta))

    val jsonSupport = mock[JsonBatchFactory]
    val verticaScanWrapper = mock[VerticaScanWrapper]
    (jsonSupport.build _).expects("path", *, *, *).returning(verticaScanWrapper)
    val readerFactory = mock[PartitionReaderFactory]
    (verticaScanWrapper.asInstanceOf[Batch].createReaderFactory _).expects().returns(readerFactory)

    val fsLayer = mock[FileStoreLayerInterface]
    (fsLayer.getFileList _).expects("path").returning(Right(Seq("file1.json", "file2.json")))

    val scan = new VerticaJsonScan(jsonReadConfig, readSetup.asInstanceOf[DSConfigSetupInterface[ReadConfig] with TableMetaInterface[ReadConfig]], jsonSupport, fsLayer)
    Try { scan.createReaderFactory() } match {
      case Success(_) => ()
      case Failure(e) => fail(e)
    }
    spark.close()
  }

}
