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
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Main  {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    // Configuration options for the connector
    val writeOpts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password"),
      "merge_key" -> conf.getString("functional-tests.merge_key")
    )

    val conn: Connection = TestUtils.getJDBCConnection(writeOpts("host"), db = writeOpts("db"), user = writeOpts("user"), password = writeOpts("password"))
    // Entry-point to all functionality in Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 5
      // Creates a table called dftest with an integer attribute
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + "(\"timestamp\" int, \"check\" int, \"end\" varchar(50), \"true\" varchar(50))")
      val insert = "insert into " + tableName + " values(2, 3, 'hello', 'world')"
      // Inserts 5 rows of the data above into dftest
      TestUtils.populateTableBySQL(stmt, insert, n)
      // Define schema of a table
      val schema = new StructType(Array(StructField("timestamp", IntegerType), StructField("check", IntegerType), StructField("end", StringType), StructField("true", StringType)))
      val data = (1 to 20).map(x => Row(x, 3, "hola", "Earth"))
      // Create a dataframe corresponding to the schema and data specified above
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Overwrite
      // Write dataframe to Vertica
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

      val df2: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).load()
      df2.rdd.foreach(x => println("VALUE: " + x))

    } finally {
      spark.close()
    }
  }
}

