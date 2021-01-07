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
      case Left(errList) =>  {
        errList(0)
      }
      case Right(config) => {
        assert(false)
        mock[ConnectorError]
      }
    }
    error
  }

}
