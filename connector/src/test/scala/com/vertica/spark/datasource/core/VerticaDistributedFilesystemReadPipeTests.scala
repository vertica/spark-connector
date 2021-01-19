import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import com.vertica.spark.config._
import com.vertica.spark.util.schema._
import com.vertica.spark.datasource.core._
import ch.qos.logback.classic.Level
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import org.apache.spark.sql.types._
import com.vertica.spark.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.util.error.SchemaErrorType._
import com.vertica.spark.util.error.JdbcErrorType._

class VerticaDistributedFilesystemReadPipeTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest{

  val tablename = "dummy"
  val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)
  val fileStoreConfig = FileStoreConfig("hdfs://example-hdfs:8020/tmp/test", Level.ERROR)

  override def afterAll() = {
  }

  it should "retrieve metadata when not provided" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig,  tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,tablename).returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => fail
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "return cached metadata" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mock[SchemaToolsInterface])

    pipe.getMetadata() match {
      case Left(err) => fail
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "return an error when there's an issue parsing schema" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
      (mockSchemaTools.readSchema _).expects(*,tablename).returning(Left(List(SchemaError(MissingConversionError, "unknown"))))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[FileStoreLayerInterface], mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => assert(err.err == SchemaDiscoveryError)
      case Right(metadata) => fail
    }
  }

  it should "call Vertica to export parquet files on pre read steps" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    val expectedAddress = fileStoreConfig.address + "/" + config.tablename
    (fileStoreLayer.removeDir _).expects(expectedAddress).returning(Right())
    (fileStoreLayer.createDir _).expects(expectedAddress).returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]
    val expectedJdbcCommand = "EXPORT TO PARQUET(directory = 'hdfs://example-hdfs:8020/tmp/test/dummy', fileSizeMB = 512, rowGroupSizeMB = 64, fileMode = '777', dirMode = '777') AS SELECT * FROM dummy;"
    (jdbcLayer.execute _).expects(expectedJdbcCommand).returning(Right())

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(false)
      case Right(_) =>
    }
  }

  it should "return an error when there's a filesystem failure" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects(*).returning(Left(ConnectorError(FileSystemError)))

    val jdbcLayer = mock[JdbcLayerInterface]

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == FileSystemError)
      case Right(_) => assert(false)
    }
  }

  it should "return an error when there's a JDBC failure" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, fileStoreConfig = fileStoreConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val fileStoreLayer = mock[FileStoreLayerInterface]
    (fileStoreLayer.removeDir _).expects(*).returning(Right())
    (fileStoreLayer.createDir _).expects(*).returning(Right())

    val jdbcLayer = mock[JdbcLayerInterface]
    (jdbcLayer.execute _).expects(*).returning(Left(JDBCLayerError(ConnectionError)))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, fileStoreLayer, jdbcLayer, mock[SchemaToolsInterface])

    pipe.doPreReadSteps() match {
      case Left(err) => assert(err.err == ExportFromVerticaError)
      case Right(_) => assert(false)
    }
  }
}
