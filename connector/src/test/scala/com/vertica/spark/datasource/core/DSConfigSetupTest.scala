import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.core.DSReadConfigSetup

import com.vertica.spark.config._
import ch.qos.logback.classic.Level
import org.scalamock.scalatest.MockFactory

import com.vertica.spark.util.error._
import com.vertica.spark.util.error.ConnectorErrorType._

class DSReadConfigSetupTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  override def beforeAll(): Unit = {
  }

  override def afterAll(): Unit = {
  }

  // Parses config expecting success
  // Calling test with fail if an error is returned
  def parseCorrectInitConfig(opts : Map[String, String]) : ReadConfig = {
    val dsConfigSetup = new DSReadConfigSetup(opts)
    val readConfig : ReadConfig = dsConfigSetup.validateAndGetConfig() match {
      case Left(err) =>  {
        assert(false)
        mock[ReadConfig]
      }
      case Right(config) => {
        config
      }
    }
    readConfig
  }

  // Parses config expecting an error
  // Calling test will fail if the config is parsed without error
  def parseErrorInitConfig(opts : Map[String, String]) : ConnectorError = {
    val dsConfigSetup = new DSReadConfigSetup(opts)
    val error : ConnectorError = dsConfigSetup.validateAndGetConfig() match {
      case Left(err) =>  {
        err
      }
      case Right(config) => {
        assert(false)
        mock[ConnectorError]
      }
    }
    error
  }

  it should "parse the logging level" in {
    var opts = Map("logging_level" -> "ERROR")
    var config = parseCorrectInitConfig(opts)
    assert(config.logLevel == Level.ERROR)

    opts = Map("logging_level" -> "DEBUG")
    config = parseCorrectInitConfig(opts)
    assert(config.logLevel == Level.DEBUG)

    opts = Map("logging_level" -> "WARNING")
    config = parseCorrectInitConfig(opts)
    assert(config.logLevel == Level.WARN)

    opts = Map("logging_level" -> "INFO")
    config = parseCorrectInitConfig(opts)
    assert(config.logLevel == Level.INFO)
  }


  it should "default to ERROR logging level" in {
    var opts = Map[String, String]()
    var config = parseCorrectInitConfig(opts)
    assert(config.logLevel == Level.ERROR)
  }

  it should "error given incorrect logging_level param" in {
    val opts = Map("logging_level" -> "OTHER")
    val err = parseErrorInitConfig(opts)
    println(err.msg)
    assert(err.err == LOGGING_LEVEL_PARSE_ERR)
  }
}
