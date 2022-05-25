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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.FileSplit
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.json.JacksonParser
import org.apache.spark.sql.connector.read.Batch
import org.apache.spark.sql.execution.datasources.PartitionedFile
import org.apache.spark.sql.execution.datasources.json.{JsonFileFormat, TextInputJsonDataSource}
import org.apache.spark.sql.execution.datasources.v2.json.JsonTable
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.sql.{DataFrame, SparkSession}

object Main {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    // Configuration options for the connector
    val readOpts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password")
    )

    // Creates a JDBC connection to Vertica
    val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))
    // Entry-point to all functionality in Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .config("spark.sql.sources.useV1SourceList", "")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called dftest with an integer attribute
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b array[array[int]], c row(float))")
      val insert = "insert into " + tableName + " values(2, array[array[1,2], array[7,3]], row(4.2))"
      // Inserts 20 rows of the value '2' into dftest
      TestUtils.populateTableBySQL(stmt, insert, n)
      // Read dftest into a dataframe

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts + ("table" -> tableName) + ("json" -> "true") + ("prevent_cleanup" -> "true"))
        .load()
      df.show()

      // println(spark.read.json("webhdfs://hdfs:50070/data/59a43f7a_fca1_4b7f_9ca7_8e183ec9ea6a/dftest/").show)

    } finally {
      spark.close()
      conn.close()
    }
  }
}