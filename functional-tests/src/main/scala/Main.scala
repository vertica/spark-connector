// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

import com.typesafe.config.{Config, ConfigFactory}
import com.vertica.spark.config._
import com.vertica.spark.datasource.core.Disable
import com.vertica.spark.functests._
import com.vertica.spark.functests.endtoend.{ComplexTypeTests, ComplexTypeTestsV10, EndToEndTests}
import org.apache.spark.sql.SparkSession
import org.scalatest.events.{Event, TestFailed, TestStarting, TestSucceeded}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Args, BeforeAndAfterAll, TestSuite}
import scopt.OParser

import scala.util.Try

case class VReporter(suiteName: String) extends org.scalatest.Reporter {
  var testCount = 0
  var succeededCount = 0
  var errCount = 0
  var testsFailed: Seq[TestFailed] = List()

  def apply(event: Event): Unit = {
    event match {
      case TestStarting(ordinal, suiteName, suiteId, suiteClassName, testName, testText, formatter, location, rerunner, payload, threadName, timeStamp) =>
        testCount += 1
      case TestSucceeded(ordinal, suiteName, suiteId, suiteClassName, testName, testText, recordedEvents, duration, formatter, location, rerunner, payload, threadName, timeStamp) =>
        println("TEST SUCCEEDED: " + testName)
        succeededCount += 1
      case testFailed: TestFailed =>
        errCount += 1
        println("TEST FAILED: " + testFailed.testName + "\n" + testFailed.message + "\n" )
        testFailed.throwable.get.printStackTrace()
        testsFailed = testsFailed :+ testFailed
      case _ =>
        println("UNEXPECTED TEST EVENT: " + event.toString)
    }
  }
}

case class TestSuiteFailed(tests: List[TestFailed] = List(), failedCount: Int, total: Int)

object Main extends App {
  def runSuite(suite: TestSuite, testName: Option[String] = None): VReporter = {
    val reporter = VReporter(suite.suiteName)
    try {
      val result = suite.run(testName, Args(reporter))
      val status = if (result.succeeds()) "passed" else "failed"
      println(suite.suiteName + "-- Test run " + status + ": " + reporter.errCount + " error(s) out of " + reporter.testCount + " test cases.")
      reporter
    } finally {
      println("IRELIA")
      sys.exit(1)
    }
  }

  val conf: Config = ConfigFactory.load()
  var readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "tls_mode" -> conf.getString("functional-tests.tlsmode"),
    "trust_store_path" -> conf.getString("functional-tests.truststorepath"),
    "trust_store_password" -> conf.getString("functional-tests.truststorepassword"))

  if (Try{conf.getString("functional-tests.aws_access_key_id")}.isSuccess) {
    readOpts = readOpts + ("aws_access_key_id" -> conf.getString("functional-tests.aws_access_key_id"))
  }
  if (Try{conf.getString("functional-tests.aws_secret_access_key")}.isSuccess) {
    readOpts = readOpts + ("aws_secret_access_key" -> conf.getString("functional-tests.aws_secret_access_key"))
  }
  if (Try{conf.getString("functional-tests.aws_session_token")}.isSuccess) {
    readOpts = readOpts + ("aws_session_token" -> conf.getString("functional-tests.aws_session_token"))
  }
  if (Try{conf.getString("functional-tests.aws_region")}.isSuccess) {
    readOpts = readOpts + ("aws_region" -> conf.getString("functional-tests.aws_region"))
  }
  if (Try{conf.getString("functional-tests.aws_credentials_provider")}.isSuccess) {
    readOpts = readOpts + ("aws_credentials_provider" -> conf.getString("functional-tests.aws_credentials_provider"))
  }
  if (Try{conf.getString("functional-tests.aws_enable_ssl")}.isSuccess) {
    readOpts = readOpts + ("aws_enable_ssl" -> conf.getString("functional-tests.aws_enable_ssl"))
  }
  if (Try{conf.getString("functional-tests.aws_endpoint")}.isSuccess) {
    readOpts = readOpts + ("aws_endpoint" -> conf.getString("functional-tests.aws_endpoint"))
  }
  if (Try{conf.getString("functional-tests.aws_enable_path_style")}.isSuccess) {
    readOpts = readOpts + ("aws_enable_path_style" -> conf.getString("functional-tests.aws_enable_path_style"))
  }

  val auth = if(Try{conf.getString("functional-tests.password")}.isSuccess) {
    readOpts = readOpts + (
      "password" -> conf.getString("functional-tests.password"),
      )
    BasicJdbcAuth(
      username = conf.getString("functional-tests.user"),
      password = conf.getString("functional-tests.password"),
    )
  } else {
    readOpts = readOpts + (
      "kerberos_service_name" -> conf.getString("functional-tests.kerberos_service_name"),
      "kerberos_host_name" -> conf.getString("functional-tests.kerberos_host_name"),
      "jaas_config_name" -> conf.getString("functional-tests.jaas_config_name")
    )
    KerberosAuth(
      username = conf.getString("functional-tests.user"),
      kerberosServiceName = conf.getString("functional-tests.kerberos_service_name"),
      kerberosHostname = conf.getString("functional-tests.kerberos_host_name"),
      jaasConfigName = conf.getString("functional-tests.jaas_config_name")
    )
  }

  val tlsConfig = JDBCTLSConfig(tlsMode = Disable, None, None, None, None)

  val jdbcConfig = JDBCConfig(host = conf.getString("functional-tests.host"),
    port = conf.getInt("functional-tests.port"),
    db = conf.getString("functional-tests.db"),
    auth = auth,
    tlsConfig = tlsConfig)
  val filename = conf.getString("functional-tests.filepath")

  val awsAuth = (sys.env.get("AWS_ACCESS_KEY_ID"), sys.env.get("AWS_SECRET_ACCESS_KEY")) match {
    case (Some(accessKeyId), Some(secretAccessKey)) => {
      Some(AWSAuth(AWSArg(Visible, EnvVar, accessKeyId), AWSArg(Secret, EnvVar, secretAccessKey)))
    }
    case (None, None) =>
      for {
        accessKeyId <- Try {
          conf.getString("functional-tests.aws_access_key_id")
        }.toOption
        secretAccessKey <- Try {
          conf.getString("functional-tests.aws_secret_access_key")
        }.toOption
      } yield AWSAuth(AWSArg(Visible, ConnectorOption, accessKeyId), AWSArg(Secret, ConnectorOption, secretAccessKey))
    case _ => None
  }

  val awsRegion = sys.env.get("AWS_DEFAULT_REGION") match {
    case Some(region) => Some(AWSArg(Visible, EnvVar, region))
    case None => {
      Try {
        conf.getString("functional-tests.aws_region")
      }.toOption match {
        case Some(region) => Some(AWSArg(Visible, ConnectorOption, region))
        case None => None
      }
    }
  }

  val awsSessionToken = sys.env.get("AWS_SESSION_TOKEN") match {
    case Some(token) => Some(AWSArg(Visible, EnvVar, token))
    case None => {
      Try {
        conf.getString("functional-tests.aws_session_token")
      }.toOption match {
        case Some(token) => Some(AWSArg(Visible, ConnectorOption, token))
        case None => None
      }
    }
  }

  val awsCredentialsProvider = sys.env.get("AWS_CREDENTIALS_PROVIDER") match {
    case Some(provider) => Some(AWSArg(Visible, EnvVar, provider))
    case None => {
      Try {
        conf.getString("functional-tests.aws_credentials_provider")
      }.toOption match {
        case Some(provider) => Some(AWSArg(Visible, ConnectorOption, provider))
        case None => None
      }
    }
  }

  val awsEnableSsl = sys.env.get("AWS_ENABLE_SSL") match {
    case Some(provider) => Some(AWSArg(Visible, EnvVar, provider))
    case None => {
      Try {
        conf.getString("functional-tests.aws_enable_ssl")
      }.toOption match {
        case Some(provider) => Some(AWSArg(Visible, ConnectorOption, provider))
        case None => None
      }
    }
  }

  val awsEndpoint = sys.env.get("AWS_ENDPOINT") match {
    case Some(provider) => Some(AWSArg(Visible, EnvVar, provider))
    case None => {
      Try {
        conf.getString("functional-tests.aws_endpoint")
      }.toOption match {
        case Some(provider) => Some(AWSArg(Visible, ConnectorOption, provider))
        case None => None
      }
    }
  }

  val awsEnablePathStyle = sys.env.get("AWS_ENABLE_PATH_STYLE") match {
    case Some(provider) => Some(AWSArg(Visible, EnvVar, provider))
    case None => {
      Try {
        conf.getString("functional-tests.aws_enable_path_style")
      }.toOption match {
        case Some(provider) => Some(AWSArg(Visible, ConnectorOption, provider))
        case None => None
      }
    }
  }

  val fileStoreConfig = FileStoreConfig(filename, "filestoretest", false, AWSOptions(awsAuth,
    awsRegion,
    awsSessionToken,
    awsCredentialsProvider,
    awsEndpoint,
    awsEnableSsl,
    awsEnablePathStyle
  ))

  val writeOpts = readOpts

  private def defaultTestSuites: String = {
    val result = Seq(
      new JDBCTests(jdbcConfig),
      new HDFSTests(fileStoreConfig, jdbcConfig),
      new CleanupUtilTests(fileStoreConfig),
      new EndToEndTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig),
      new ComplexTypeTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig)
    ).mkString("\n")
    result + "\n"
  }

  case class Options(large: Boolean = false, v10: Boolean = false, suite: String = "", testName: String = "")
  val builder = OParser.builder[Options]
  val optParser = {
    import builder._
    OParser.sequence(
      note("By default, the following test suites will be run:\n" + defaultTestSuites),
      note("Use the following options to alter the test suites:\n"),
      opt[Unit]('l', "large")
        .optional()
        .action((_, testList: Options) => testList.copy(large = true))
        .text("Add LargeDataTests to run."),
      opt[Unit]('v', "v10")
        .action((_, options: Options) => options.copy(v10 = true))
        .text("Replace ComplexDataTypeTests with ComplexDataTypeTestsV10 for Vertica 10.x."),
      opt[String]('s', "suite")
        .action((value: String, options: Options) => options.copy(suite = value))
        .text("Specify a specific test suite name to run."),
      opt[String]('t', "test")
        .action((value: String, options: Options) => options.copy(testName = value.trim))
        .text("Specify a test name in a suite to run. Require -s to be given."),
     help('h', "help")
        .text("Print help"),
    )
  }

  OParser.parse(optParser, args, Options()) match {
    case Some(options) => executeTests(options)
    case None => sys.exit(1)
  }

  def executeTests(options: Options): Unit = {
    val suitesForExecution = buildTestSuitesForExecution(options)
    val testName = getTestName(options)

    val results =  suitesForExecution.map(suite => {runSuite(suite, testName)})

    println("SUMMARY:")
    println(s"Test suites executed, in order: \n" + results.map(_.suiteName).mkString(" -> "))
    val exitCode = results.map(result => printResultAndGetFailedCount(result)).sum
    sys.exit(exitCode)
  }

  private def getTestName(options: Options): Option[String] = {
    if (options.testName.isBlank) {
      None
    } else {
      val testName = if (!options.testName.startsWith("should")) {
        "should " + options.testName
      } else {
        options.testName
      }
      Some(testName)
    }
  }

  private def buildTestSuitesForExecution(options: Options): Seq[AnyFlatSpec with BeforeAndAfterAll] = {
    var testSuites =  Seq(
      new JDBCTests(jdbcConfig),
      new HDFSTests(fileStoreConfig, jdbcConfig),
      new CleanupUtilTests(fileStoreConfig),
      new EndToEndTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig),
    )

    testSuites = if (options.v10) testSuites :+ new ComplexTypeTestsV10(readOpts, writeOpts, jdbcConfig, fileStoreConfig)
    else testSuites :+ new ComplexTypeTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig)

    testSuites = if (options.large) testSuites :+ new LargeDataTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig) else testSuites

    if(options.suite.isBlank) {
      testSuites
    } else {
      val result = testSuites.filter(_.suiteName.equals(options.suite))
      assert(result.nonEmpty, s"Test suite ${options.suite} does not exist.")
      result
    }
  }

  private def printResultAndGetFailedCount(result: VReporter) = {
    val testFailed = result.errCount > 0
    val status = if (testFailed) "FAILED" else "PASSED"
    println(s"${result.suiteName} $status")
    if (testFailed) {
      result.testsFailed.foreach(failedTest => {
        println(s" - FAILED: ${failedTest.testName}, message:")
        println(s"  ${failedTest.message}")
      })
    }
    result.errCount
  }

}
