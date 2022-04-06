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

import com.typesafe.config.{Config, ConfigFactory}
import com.vertica.spark.util.schema.MetadataKey
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import java.sql.Connection

object Main {

  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    // Configuration options for the connector
    val options = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password")
    )
    // Entry-point to all functionality in Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()
    val conn: Connection = TestUtils.getJDBCConnection(options("host"), db = options("db"), user = options("user"), password = options("password"))
    TestUtils.createTableBySQL(conn,"dftest","create table dftest (a SET[int])")
    TestUtils.populateTableBySQL(conn.createStatement(), "insert into dftest values (SET[1])", 1)
    try {
      /**
       * SET is not a defined data type in JDBC and thus is converted to Array.
       * In Spark, we differentiate arrays from sets by marking the column's metadata.
       * */
      readSetFromVertica(spark, options)
    } finally {
      conn.close()
      spark.close()
    }
  }

  def readSetFromVertica(spark: SparkSession, options: Map[String, String]): Unit = {
    val readOpts = options + ("table" -> "dftest")
    // Load a set from Vertica as array and display it
    val df = spark.read.format("com.vertica.spark.datasource.VerticaSource")
      .options(readOpts)
      .load()
    df.show()
    val col1 = df.schema.fields(0)
    // Since set are ingested into Spark as array, we mark such array as set in it's metadata
    println(s"Is ${col1.name} SET type: " + col1.metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
    println()
  }
}
