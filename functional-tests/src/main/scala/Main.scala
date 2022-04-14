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
import org.scalatest.events.{Event, TestFailed, TestStarting, TestSucceeded}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.{Args, TestSuite}
import scopt.OParser

import scala.collection.mutable.ListBuffer
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
  var testSuitesFailed: List[VReporter] = List()
  def runSuite(suite: TestSuite, testName: Option[String] = None): Boolean = {
    suite.suiteName
    val reporter = VReporter(suite.suiteName)
    val result = suite.run(testName, Args(reporter))
    val status = if (result.succeeds()) "passed" else {
      testSuitesFailed = testSuitesFailed :+ reporter
      "failed"
    }
    println(suite.suiteName + "-- Test run "+ status +": " + reporter.errCount + " error(s) out of " + reporter.testCount + " test cases.")
    result.succeeds()
  }

  case class Options(large: Boolean = false, v10: Boolean = false, suite: String = "", test: String = "")

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
  }
  else {
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

  val builder = OParser.builder[Options]
  val optParser = {
    import builder._
    OParser.sequence(
      opt[Unit]('l', "large")
        .optional()
        .action((_, options: Options) => options.copy(large = true))
        .text("Include large data tests"),
      opt[Unit]('v', "v10")
        .action((_, options: Options) => options.copy(v10 = true))
        .text("Use complex type tests for Vertica 10"),
      opt[String]('s', "suite")
        .action((value: String, options: Options) => options.copy(suite = value))
        .text("A specific test suite name to run"),
      opt[String]('t', "test")
        .action((value: String, options: Options) => options.copy(test = value))
        .text("Name of a specific test in a suite to run. Will error if option -s is not given."),
     help('h', "help")
        .text("Print help"),
    )
  }

  OParser.parse(optParser, args, Options()) match {
    case Some(options) => executeTests(options)
    case None => sys.exit(1)
  }

  def executeTests(options: Options): Unit = {
    val baseTestSuites = ListBuffer(
      new JDBCTests(jdbcConfig),
      new HDFSTests(fileStoreConfig, jdbcConfig),
      new CleanupUtilTests(fileStoreConfig),
      new EndToEndTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig)
    )
    if (options.v10) baseTestSuites.append(new ComplexTypeTestsV10(readOpts, writeOpts, jdbcConfig, fileStoreConfig))
    else baseTestSuites.append(new ComplexTypeTests(readOpts, writeOpts, jdbcConfig, fileStoreConfig))
    if (options.large) baseTestSuites.append(new LargeDataTests(readOpts, writeOpts, jdbcConfig))

    val suitesForExecution = if (options.suite.isBlank) baseTestSuites.toList
    else baseTestSuites.filter(_.suiteName.equals(options.suite)).toList
    assert(suitesForExecution.nonEmpty, s"Test suite ${options.suite} does not exist.")
    val testName: Option[String] = if (options.test.isBlank) None else Some("should " + options.test)
    val result = Try {
      suitesForExecution.foreach(suite => {
        runSuite(suite, testName)
      })
    }

    if(testSuitesFailed.isEmpty) {
      println(s"ALL PASSED")
      println(s"Test suites executed, in order: \n" + suitesForExecution.map(_.suiteName).mkString(" -> "))
      sys.exit(0)
    }else {
      println("TESTS FAILED")
      println(s"Test suites executed, in order: \n" + suitesForExecution.map(_.suiteName).mkString(" -> "))
      testSuitesFailed.foreach(report => {
        println(s"SUMMARY: ${report.suiteName} failed ${report.errCount} out of ${report.testCount} tests")
        report.testsFailed.foreach(failedTest => {
          println(s" - FAILED: ${failedTest.testName}, message:")
          println(s"  ${failedTest.message}")
        })
      })
      sys.exit(1)
    }
  }
}
