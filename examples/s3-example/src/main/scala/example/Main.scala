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

package example

import java.sql.Connection
import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.{DataFrame, SparkSession}
import scala.util.Try

object Main extends App {
  override def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    var readOpts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password"),
      "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"},
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
    if (Try{conf.getString("functional-tests.aws_session_token")}.isSuccess) {
      readOpts = readOpts + ("aws_session_token" -> conf.getString("functional-tests.aws_session_token"))
    }
    if (Try{conf.getString("functional-tests.aws_credentials_provider")}.isSuccess) {
      readOpts = readOpts + ("aws_credentials_provider" -> conf.getString("functional-tests.aws_credentials_provider"))
    }
    if (Try{conf.getString("functional-tests.aws_endpoint")}.isSuccess) {
      readOpts = readOpts + ("aws_endpoint" -> conf.getString("functional-tests.aws_endpoint"))
    }
    if (Try{conf.getString("functional-tests.aws_enabled_ssl")}.isSuccess) {
      readOpts = readOpts + ("aws_enable_ssl" -> conf.getString("functional-tests.aws_enable_ssl"))
    }

    val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int)")

      val insert = "insert into " + tableName + " values(2)"
      TestUtils.populateTableBySQL(stmt, insert, n)

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      df.rdd.foreach(x => println("VALUE: " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }
}
