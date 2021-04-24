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
import com.vertica.spark.config.{BasicJdbcAuth, DistributedFilesystemReadConfig, FileStoreConfig, JDBCConfig, KerberosAuth, TableName, VerticaMetadata}
import com.vertica.spark.config.{FileStoreConfig, JDBCConfig}
import ch.qos.logback.classic.Level
import com.vertica.spark.perftests.{BothMode, DataGenUtils, PerformanceTestSuite, ReadMode, WriteMode}
import org.apache.spark.sql.SparkSession

object Main extends App {
  private val spark = SparkSession.builder()
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  val conf: Config = ConfigFactory.load()
  var readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "max_file_size" -> conf.getString("functional-tests.max_file_size"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "hdfs_url" -> conf.getString("functional-tests.filepath"),
    "num_partitions" -> conf.getString("functional-tests.num_partitions")
  )
  val auth = if(conf.getString("functional-tests.password").nonEmpty) {
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

  // read/write/both

  val colCounts = conf.getString("functional-tests.colCounts")
  val rowCounts = conf.getString("functional-tests.rowCounts")
  val runCount = conf.getInt("functional-tests.runCount")

  val rowGroupSizes = conf.getString("functional-tests.max_row_group_size")
  val optList = rowGroupSizes.split(",").map(rowGroupSize => {
    readOpts + ("max_row_group_size" -> rowGroupSize)
  })

  val testModeStr = conf.getString("functional-tests.testMode")
  val testMode = testModeStr match {
    case "read" => ReadMode()
    case "write" => WriteMode()
    case "both" => BothMode()
    case _ => throw new Exception("Invalid test mode, must be 'read', 'write' or 'both'")
  }

  val runJdbcComparison = conf.getBoolean("functional-tests.compareJdbc")
  val runV1Comparison = conf.getBoolean("functional-tests.compareV1")

  new PerformanceTestSuite(spark).runAndTimeTests(optList,
    colCounts,
    rowCounts,
    runCount,
    testMode,
    runJdbcComparison,
    runV1Comparison,
    readOpts("num_partitions").toInt)

}
