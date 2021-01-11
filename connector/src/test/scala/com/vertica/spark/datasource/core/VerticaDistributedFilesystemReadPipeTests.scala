import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import org.scalamock.scalatest.MockFactory
import java.sql.ResultSet
import java.sql.ResultSetMetaData

import com.vertica.spark.config._
import com.vertica.spark.util.schema._
import com.vertica.spark.datasource.core._

import ch.qos.logback.classic.Level
import org.apache.spark.sql.types._

import com.vertica.spark.jdbc.JdbcLayerInterface

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.util.error.SchemaErrorType._

class VerticaDistributedFilesystemReadPipeTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest{

  val tablename = "dummy"
  val jdbcConfig = JDBCConfig("1.1.1.1", 1234, "test", "test", "test", Level.ERROR)

  override def afterAll() = {
  }

  it should "retrieve metadata when not provided" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
    (mockSchemaTools.readSchema _).expects(*,tablename).returning(Right(new StructType()))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => assert(false)
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "return cached metadata" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, tablename = tablename, metadata = Some(new VerticaMetadata(new StructType())))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[JdbcLayerInterface], mock[SchemaToolsInterface])

    pipe.getMetadata() match {
      case Left(err) => assert(false)
      case Right(metadata) => assert(metadata.schema == new StructType())
    }
  }

  it should "return an error when there's an issue parsing schema" in {
    val config = DistributedFilesystemReadConfig(logLevel = Level.ERROR, jdbcConfig = jdbcConfig, tablename = tablename, metadata = None)

    val mockSchemaTools = mock[SchemaToolsInterface]
      (mockSchemaTools.readSchema _).expects(*,tablename).returning(Left(List(SchemaError(MissingConversionError, "unknown"))))

    val pipe = new VerticaDistributedFilesystemReadPipe(config, mock[JdbcLayerInterface], mockSchemaTools)

    pipe.getMetadata() match {
      case Left(err) => assert(err.err == SchemaDiscoveryError)
      case Right(metadata) => assert(false)
    }
  }

}
