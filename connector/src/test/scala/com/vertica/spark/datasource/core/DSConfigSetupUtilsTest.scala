import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.core.DSReadConfigSetup

import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._
import com.vertica.spark.datasource.core.DSConfigSetupUtils

class DSReadConfigSetupUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  def getResultOrAssert[ResultType](either : Either[_,ResultType])  = {
    if(either.isLeft) {
      assert(false)
    }
    either.right.get
  }

  def getErrorOrAssert[ErrorType](either : Either[ErrorType,_])  = {
    if(either.isRight) {
      assert(false)
    }
    either.left.get
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
    assert(err.err == InvalidLoggingLevel)
  }

  it should "parse the host name" in {
    val opts = Map("host" -> "1.1.1.1")
    val host = getResultOrAssert[String](DSConfigSetupUtils.getHost(opts))
    assert(host == "1.1.1.1")
  }

  it should "fail with missing host name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getHost(opts))
    assert(err.err == HostMissingError)
  }

  it should "parse the port" in {
    val opts = Map("port" -> "1234")
    val port = getResultOrAssert[Integer](DSConfigSetupUtils.getPort(opts))
    assert(port == 1234)
  }

  it should "defaults to port 5543" in {
    val opts = Map[String, String]()
    val port = getResultOrAssert[Integer](DSConfigSetupUtils.getPort(opts))
    assert(port == 5543)
  }

  it should "error with invalid port input" in {
    var opts = Map("port" -> "abc123")
    var err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.err == InvalidPortError)

    opts = Map("port" -> "1.1")
    err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPort(opts))
    assert(err.err == InvalidPortError)
  }

  it should "parse the db name" in {
    val opts = Map("db" -> "testdb")
    val db = getResultOrAssert[String](DSConfigSetupUtils.getDb(opts))
    assert(db == "testdb")
  }

  it should "fail with missing db name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getDb(opts))
    assert(err.err == DbMissingError)
  }

  it should "parse the username" in {
    val opts = Map("user" -> "testuser")
    val user = getResultOrAssert[String](DSConfigSetupUtils.getUser(opts))
    assert(user == "testuser")
  }

  it should "fail with missing username" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getUser(opts))
    assert(err.err == UserMissingError)
  }

  it should "parse the table name" in {
    val opts = Map("tablename" -> "tbl")
    val table = getResultOrAssert[String](DSConfigSetupUtils.getTablename(opts))
    assert(table == "tbl")
  }

  it should "fail with missing table name" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getTablename(opts))
    assert(err.err == TablenameMissingError)
  }

  it should "parse the password" in {
    val opts = Map("password" -> "pass")
    val pass = getResultOrAssert[String](DSConfigSetupUtils.getPassword(opts))
    assert(pass == "pass")
  }

  it should "fail with missing password" in {
    val opts = Map[String, String]()
    val err = getErrorOrAssert[ConnectorError](DSConfigSetupUtils.getPassword(opts))
    assert(err.err == PasswordMissingError)
  }
}
