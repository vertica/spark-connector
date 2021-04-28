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
import com.vertica.spark.config.{BasicJdbcAuth, DistributedFilesystemReadConfig, FileStoreConfig, JDBCConfig, JDBCSSLConfig, KerberosAuth, TableName, VerticaMetadata}
import com.vertica.spark.functests.{CleanupUtilTests, EndToEndTests, HDFSTests, JDBCTests}
import com.vertica.spark.functests.{CleanupUtilTests, EndToEndTests, HDFSTests, JDBCTests, LargeDataTests}
import scala.util.Try

object Main extends App {
  val conf: Config = ConfigFactory.load()
  var readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )
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

  val sslConfig = JDBCSSLConfig(ssl = false, None, None, None, None)

  val jdbcConfig = JDBCConfig(host = conf.getString("functional-tests.host"),
                              port = conf.getInt("functional-tests.port"),
                              db = conf.getString("functional-tests.db"),
                              auth = auth,
                              sslConfig = sslConfig)

  new JDBCTests(jdbcConfig).execute()

  val filename = conf.getString("functional-tests.filepath")
  val dirTestFilename = conf.getString("functional-tests.dirpath")
  new HDFSTests(
    FileStoreConfig(filename),
    FileStoreConfig(dirTestFilename),
    jdbcConfig
  ).execute()

  new CleanupUtilTests(
    FileStoreConfig(filename)
  ).execute()

  val writeOpts = readOpts
  new EndToEndTests(readOpts, writeOpts, jdbcConfig).execute()

  if (args.length == 1 && args(0) == "Large") {
    new LargeDataTests(readOpts, writeOpts, jdbcConfig).execute()
  }
}
