import cats.data.Validated.{Invalid, Valid}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import com.vertica.spark.datasource.core.DSReadConfigSetup
import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.datasource.core._
import org.apache.spark.sql.types._

class DSConfigSetupTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }


  // Parses config expecting success
  // Calling test with fail if an error is returned

  def parseCorrectInitConfig(opts : Map[String, String], dsReadConfigSetup: DSReadConfigSetup) : ReadConfig = {
    val readConfig : ReadConfig = dsReadConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(err) =>
        fail
        mock[ReadConfig]
      case Valid(config) =>
        config
    }
    readConfig
  }

  // Parses config expecting an error
  // Calling test will fail if the config is parsed without error
  def parseErrorInitConfig(opts : Map[String, String], dsReadConfigSetup: DSReadConfigSetup) : Seq[ConnectorError] = {
    dsReadConfigSetup.validateAndGetConfig(opts) match {
      case Invalid(errList) => errList.toNonEmptyList.toList
      case Valid(_) =>
        fail
        List[ConnectorError]()
    }
  }


  it should "parse a valid read config" in {
    val opts = Map("logging_level" -> "ERROR",
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "table" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipe = mock[DummyReadPipe]
    (mockPipe.getMetadata _).expects().returning(Right(VerticaMetadata(new StructType))).once()
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]
    (mockPipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val dsReadConfigSetup = new DSReadConfigSetup(mockPipeFactory)

    parseCorrectInitConfig(opts, dsReadConfigSetup) match {
      case config: DistributedFilesystemReadConfig =>
        assert(config.jdbcConfig.host == "1.1.1.1")
        assert(config.jdbcConfig.port == 1234)
        assert(config.jdbcConfig.db == "testdb")
        assert(config.jdbcConfig.username == "user")
        assert(config.jdbcConfig.password == "password")
        assert(config.tablename.getFullTableName == "tbl")
        assert(config.logLevel == Level.ERROR)
        config.metadata match {
          case Some(metadata) => assert(metadata.schema == new StructType())
          case None => fail
        }
    }
  }

  it should "Return several parsing errors" in {
    // Should be one error from the jdbc parser for the port and one for the missing log level
    val opts = Map("logging_level" -> "invalid",
                   "host" -> "1.1.1.1",
                   "db" -> "testdb",
                   "port" -> "asdf",
                   "user" -> "user",
                   "password" -> "password",
                   "tablename" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    var dsReadConfigSetup = new DSReadConfigSetup(mock[VerticaPipeFactoryInterface])

    val errSeq = parseErrorInitConfig(opts, dsReadConfigSetup)
    assert(errSeq.size == 2)
    assert(errSeq.exists(err => err.err == InvalidPortError))
    assert(errSeq.exists(err => err.err == InvalidLoggingLevel))
  }

  it should "Return error when there's a problem retrieving metadata" in {

    val opts = Map("logging_level" -> "ERROR",
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password",
                   "tablename" -> "tbl",
                   "staging_fs_url" -> "hdfs://test:8020/tmp/test"
    )

    // Set mock pipe
    val mockPipe = mock[DummyReadPipe]
    (mockPipe.getMetadata _).expects().returning(Left(ConnectorError(SchemaDiscoveryError))).once()
    val mockPipeFactory = mock[VerticaPipeFactoryInterface]
    (mockPipeFactory.getReadPipe _).expects(*).returning(mockPipe)

    val dsReadConfigSetup = new DSReadConfigSetup(mockPipeFactory)

    val errSeq = parseErrorInitConfig(opts, dsReadConfigSetup)
    assert(errSeq.size == 1)
    assert(errSeq.exists(err => err.err == SchemaDiscoveryError))
  }
}
