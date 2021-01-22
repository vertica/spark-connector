import cats.data.Validated.{Invalid, Valid}
import cats.data.{NonEmptyChain, ValidatedNec}
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory
import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.datasource.core.DSConfigSetupUtils

class DSReadConfigSetupUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  def getResultOrAssert[ResultType](validation : ValidatedNec[_,ResultType]): ResultType = {
    validation match {
      case Invalid(_) => {
        fail
      }
      case Valid(result) => result
    }
  }

  def getErrorOrAssert[ErrorType](validation : ValidatedNec[ErrorType,_]): NonEmptyChain[ErrorType] = {
    validation match {
      case Invalid(errors) => errors
      case Valid(result) => {
        fail
      }
    }
  }

  it should "parse the logging level" in {
    var opts = Map("logging_level" -> "ERROR")
    var level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.ERROR)

    opts = Map("logging_level" -> "DEBUG")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.DEBUG)

    opts = Map("logging_level" -> "WARNING")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.WARN)

    opts = Map("logging_level" -> "INFO")
    level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.INFO)
  }

  it should "default to ERROR logging level" in {
    var opts = Map[String, String]()
    val level = getResultOrAssert[Level](DSConfigSetupUtils.getLogLevel(opts))
    assert(level == Level.ERROR)
  }

  it should "error given incorrect logging_level param" in {
    val opts = Map("logging_level" -> "OTHER")
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getLogLevel(opts))
    assert(err.toNonEmptyList.head.err == InvalidLoggingLevel)
  }

  it should "parse the host name" in {
    val opts = Map("host" -> "1.1.1.1")
    val host = getResultOrAssert[String](DSConfigSetupUtils.getHost(opts))
    assert(host == "1.1.1.1")
  }

  it should "fail with missing host name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getHost(opts))
    assert(err.toNonEmptyList.head.err == HostMissingError)
  }

  it should "parse the port" in {
    val opts = Map("port" -> "1234")
    val port = getResultOrAssert[Int](DSConfigSetupUtils.getPort(opts))
    assert(port == 1234)
  }

  it should "defaults to port 5543" in {
    val opts = Map[String, String]()
    val port = getResultOrAssert[Int](DSConfigSetupUtils.getPort(opts))
    assert(port == 5433)
  }

  it should "error with invalid port input" in {
    var opts = Map("port" -> "abc123")
    var err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.toNonEmptyList.head.err == InvalidPortError)

    opts = Map("port" -> "1.1")
    err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.toNonEmptyList.head.err == InvalidPortError)
  }

  it should "parse the db name" in {
    val opts = Map("db" -> "testdb")
    val db = getResultOrAssert[String](DSConfigSetupUtils.getDb(opts))
    assert(db == "testdb")
  }

  it should "fail with missing db name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getDb(opts))
    assert(err.toNonEmptyList.head.err == DbMissingError)
  }

  it should "parse the username" in {
    val opts = Map("user" -> "testuser")
    val user = getResultOrAssert[String](DSConfigSetupUtils.getUser(opts))
    assert(user == "testuser")
  }

  it should "fail with missing username" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getUser(opts))
    assert(err.toNonEmptyList.head.err == UserMissingError)
  }

  it should "parse the table name" in {
    val opts = Map("tablename" -> "tbl")
    val table = getResultOrAssert[String](DSConfigSetupUtils.getTablename(opts))
    assert(table == "tbl")
  }

  it should "fail with missing table name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getTablename(opts))
    assert(err.toNonEmptyList.head.err == TablenameMissingError)
  }

  it should "parse the password" in {
    val opts = Map("password" -> "pass")
    val pass = getResultOrAssert[String](DSConfigSetupUtils.getPassword(opts))
    assert(pass == "pass")
  }

  it should "fail with missing password" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPassword(opts))
    assert(err.toNonEmptyList.head.err == PasswordMissingError)
  }

  it should "parse the staging filesystem url" in {
    val opts = Map[String, String]("staging_fs_url" -> "hdfs://test:8020/tmp/test")
    val url = getResultOrAssert [String](DSConfigSetupUtils.getStagingFsUrl(opts))
    assert(url == "hdfs://test:8020/tmp/test")
  }

  it should "fail with missing staging filesystem url" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getStagingFsUrl(opts))
    assert(err.toNonEmptyList.head.err == StagingFsUrlMissingError)
  }
}
