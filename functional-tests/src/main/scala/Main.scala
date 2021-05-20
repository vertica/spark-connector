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

import Main.conf
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import com.vertica.spark.config.{AWSAuth, AWSOptions, BasicJdbcAuth, DistributedFilesystemReadConfig, FileStoreConfig, JDBCConfig, JDBCTLSConfig, KerberosAuth, TableName, VerticaMetadata}
import com.vertica.spark.datasource.core.Disable
import com.vertica.spark.functests.{CleanupUtilTests, EndToEndTests, HDFSTests, JDBCTests}
import com.vertica.spark.functests.{CleanupUtilTests, EndToEndTests, HDFSTests, JDBCTests, LargeDataTests}
import org.scalatest.{Args, DispatchReporter, TestSuite}
import org.scalatest.events.{Event, TestFailed, TestStarting, TestSucceeded}
import org.scalatest.tools.StandardOutReporter

import scala.util.Try

class VReporter extends org.scalatest.Reporter {
  var testCount = 0
  var succeededCount = 0
  var errCount = 0

  def apply(event: Event): Unit = {
    event match {
      case TestStarting(ordinal, suiteName, suiteId, suiteClassName, testName, testText, formatter, location, rerunner, payload, threadName, timeStamp) =>
        testCount += 1
      case TestSucceeded(ordinal, suiteName, suiteId, suiteClassName, testName, testText, recordedEvents, duration, formatter, location, rerunner, payload, threadName, timeStamp) =>
        println("TEST SUCCEEDED: " + testName)
        succeededCount += 1
      case TestFailed(ordinal, message, suiteName, suiteId, suiteClassName, testName, testText, recordedEvents, analysis, throwable, duration, formatter, location, rerunner, payload, threadName, timeStamp) =>
        errCount += 1
        println("TEST FAILED: " + testName + "\n" + message)
      case _ =>
        println("UNEXPECTED TEST EVENT: " + event.toString)
    }
  }
}

object Main extends App {
  def runSuite(suite: TestSuite): Unit = {
    val reporter = new VReporter()
    val result = suite.run(None, Args(reporter))
    if(!result.succeeds()) {
      throw new Exception("Test run failed: " + reporter.errCount + " error(s) out of " + reporter.testCount + " test cases.")
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
    "trust_store_password" -> conf.getString("functional-tests.truststorepassword"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )

  if (Try{conf.getString("functional-tests.aws_access_key_id")}.isSuccess) {
    readOpts = readOpts + ("aws_access_key_id" -> conf.getString("functional-tests.aws_access_key_id"))
  }
  if (Try{conf.getString("functional-tests.aws_secret_access_key")}.isSuccess) {
    readOpts = readOpts + ("aws_secret_access_key" -> conf.getString("functional-tests.aws_secret_access_key"))
  }
  if (Try{conf.getString("functional-tests.aws_region")}.isSuccess) {
    readOpts = readOpts + ("aws_region" -> conf.getString("functional-tests.aws_region"))
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

  (new JDBCTests(jdbcConfig)).execute()

  val filename = conf.getString("functional-tests.filepath")
  val awsAuth = (sys.env.get("AWS_ACCESS_KEY_ID"), sys.env.get("AWS_SECRET_ACCESS_KEY")) match {
    case (Some(accessKeyId), Some(secretAccessKey)) => Some(AWSAuth(accessKeyId, secretAccessKey))
    case (None, None) =>
      for {
        accessKeyId <- Try{conf.getString("functional-tests.aws_access_key_id")}.toOption
        secretAccessKey <- Try{conf.getString("functional-tests.aws_secret_access_key")}.toOption
      } yield AWSAuth(accessKeyId, secretAccessKey)
    case _ => None
  }

  val fileStoreConfig = FileStoreConfig(filename, AWSOptions(
    awsAuth, sys.env.get("AWS_DEFAULT_REGION") match {
      case Some(region) => Some(region)
      case None => Try{conf.getString("functional-tests.aws_region")}.toOption
    }))
  (new HDFSTests(
    fileStoreConfig,
    jdbcConfig
  )).execute()

  (new CleanupUtilTests(
    fileStoreConfig
  )).execute()

  val writeOpts = readOpts
  (new EndToEndTests(readOpts, writeOpts, jdbcConfig)).execute()

  if (args.length == 1 && args(0) == "Large") {
    (new LargeDataTests(readOpts, writeOpts, jdbcConfig)).execute()
  }
}
