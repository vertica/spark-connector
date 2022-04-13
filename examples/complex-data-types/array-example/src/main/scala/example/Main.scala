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

    /*
    * Make a JDBC connection to Vertica. The connector does not support nested array at the moment so we will use this
    * to query for nested array.
    * */
    val conn: Connection = TestUtils.getJDBCConnection(options("host"), db = options("db"), user = options("user"), password = options("password"))

    try {
      save1DArrayToVertica(spark, options)
      saveNestedArrayToVertica(spark, options)
      load1DArrayFromVertica(options, spark)
      queryNestedArrayFromVertica(conn)
    } finally {
      conn.close()
      spark.close()
    }
  }

  private def queryNestedArrayFromVertica(conn: Connection): Unit = {
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(s"SELECT * FROM nested_array_test")
    println("JDBC query result: ")
    while (rs.next) {
      println(s"[${rs.getString(1)}, ${rs.getArray(2)}]")
    }
  }

  private def load1DArrayFromVertica(options: Map[String, String], spark: SparkSession): Unit = {
    val readOpts = options + ("table" -> "1D_array_test")
    // Load 1D array table from Vertica and display it
    val df = spark.read.format("com.vertica.spark.datasource.VerticaSource")
      .options(readOpts)
      .load()
    df.show()
  }

  def save1DArrayToVertica(spark: SparkSession, options: Map[String, String]): Unit = {
    val tableName = "1D_array_test"
    // Table name needs to be specified in option
    val writeOpts = options + ("table" -> tableName)
    // Define schema of a table with a 1D array column
    val schema = new StructType(Array(StructField("col1", ArrayType(IntegerType))))
    // data
    val data = Seq(Row(Array(1, 1, 1, 2, 2, 2)))
    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
    // Write dataframe to Vertica. note the data source
    df.write.format("com.vertica.spark.datasource.VerticaSource")
      .options(writeOpts)
      .mode(SaveMode.Overwrite)
      .save()
  }

  def saveNestedArrayToVertica(spark: SparkSession, options: Map[String, String]): Unit = {
    val tableName = "nested_array_test"
    // Table name needs to be specified in option
    val writeOptions = options + ("table" -> tableName)

    /*
    * Defining a schema of a table with a nested array column.
    * Vertica requires all tables to have at least one native type column. Nested arrays are complex types thus
    * we need col1 define as so.
    * */
    val schema = new StructType(Array(
      StructField("col1", StringType),
      StructField("col2", ArrayType(ArrayType(IntegerType)))))
    // data
    val data = Seq(Row(
      "vertica",
      Array(Array(9, 9, 9, 4, 4, 4))))
    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
    // Write dataframe to Vertica. note the data source
    df.write.format("com.vertica.spark.datasource.VerticaSource")
      .options(writeOptions)
      .mode(SaveMode.Overwrite)
      .save()
  }
}
