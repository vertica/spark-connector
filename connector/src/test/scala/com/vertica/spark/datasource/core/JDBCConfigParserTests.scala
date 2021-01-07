import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.core.DSReadConfigSetup

import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._

import com.vertica.spark.datasource.core.JDBCConfigParser


class JDBCConfigParserTests extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {

  it should "parse the JDBC config" in {
    val opts = Map(
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password"
    )

    val parser = new JDBCConfigParser()

    val logLevel : Option[Level] = Some(Level.ERROR)

    parser.validateAndGetJDBCConfig(opts, logLevel) match {
      case Left(errSeq) => {
        assert(false)
      }
      case Right(jdbcConfig) => {
        assert(jdbcConfig.host == "1.1.1.1")
        assert(jdbcConfig.port == 1234)
        assert(jdbcConfig.db == "testdb")
        assert(jdbcConfig.username == "user")
        assert(jdbcConfig.password == "password")
        println(jdbcConfig.logLevel)
        assert(jdbcConfig.logLevel == logLevel.get)
      }
    }
  }

  it should "return several configuration errors" in {
    val opts = Map(
                   "host" -> "1.1.1.1"
    )

    val parser = new JDBCConfigParser()

    val logLevel : Option[Level] = Some(Level.ERROR)

    parser.validateAndGetJDBCConfig(opts, logLevel) match {
      case Left(errSeq) => {
        assert(errSeq.size == 3)
        assert(!errSeq.filter(err => err.err == UserMissingError).isEmpty)
        assert(!errSeq.filter(err => err.err == PasswordMissingError).isEmpty)
        assert(!errSeq.filter(err => err.err == DbMissingError).isEmpty)
      }
      case Right(jdbcConfig) => {
        assert(false) // should not succeed
      }
    }
  }

  it should "return all possible configuration errors" in {
    val opts = Map[String, String]()

    val parser = new JDBCConfigParser()

    val logLevel : Option[Level] = Some(Level.ERROR)

    parser.validateAndGetJDBCConfig(opts, logLevel) match {
      case Left(errSeq) => {
        assert(errSeq.size == 4)
      }
      case Right(jdbcConfig) => {
        assert(false) // should not succeed
      }
    }
  }

  it should "fail when not being given a logging level" in {
    val opts = Map(
                   "host" -> "1.1.1.1",
                   "port" -> "1234",
                   "db" -> "testdb",
                   "user" -> "user",
                   "password" -> "password"
    )

    val parser = new JDBCConfigParser()

    val logLevel : Option[Level] = None

    parser.validateAndGetJDBCConfig(opts, logLevel) match {
      case Left(errSeq) => {
        // Should return empty list of errors as the logging level is externally parsed. Configuration invalid but error is no errors from the JDBC parser to report
        assert(errSeq.size == 0)
      }
      case Right(jdbcConfig) => {
        assert(false) // should not succeed
      }
    }
  }
}
