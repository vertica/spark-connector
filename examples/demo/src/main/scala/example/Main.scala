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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

class DemoCases(conf: Config) {
  val readOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )

  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  def basicRead(): Unit = {
    println("DEMO: Reading.")

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int)")

      val insert = "insert into " + tableName + " values(2)"
      TestUtils.populateTableBySQL(stmt, insert, n)

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      df.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }

  def columnPushdown(): Unit = {
    println("DEMO: Reading with column pushdown.")

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")

      val insert = "insert into " + tableName + " values(2, 3)"
      TestUtils.populateTableBySQL(stmt, insert, n)

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      val dfCol = df.select("b")

      dfCol.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }

  def filterPushdown(): Unit = {
    println("DEMO: Reading with column pushdown.")

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 20
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")

      val insert = "insert into " + tableName + " values(2, 3)"
      TestUtils.populateTableBySQL(stmt, insert, n)
      val insert2 = "insert into " + tableName + " values(5, 1)"
      TestUtils.populateTableBySQL(stmt, insert2, n)

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      val dfGreater = df.filter("b > 4")
      dfGreater.rdd.foreach(x => println("DEMO: Read value " + x))

      val dfEqual = df.filter("a == 1")
      dfEqual.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }
}

object Main extends App {
  def noCase(): Unit = println("DEMO: No case defined with that name")

  val conf: Config = ConfigFactory.load()

  val demoCases = new DemoCases(conf)

  val m: Map[String, () => Unit] = Map(
    "basicRead" -> demoCases.basicRead,
    "columnPushdown" -> demoCases.columnPushdown,
    "filterPushdown" -> demoCases.filterPushdown
  )

  if(args.size != 1) {
    println("DEMO: Please enter name of demo case to run.")
  }
  else {
    val f: () => Unit = m.getOrElse(args.head, noCase)
    f()
  }

  /*
  val writeOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password"),
    "logging_level" -> {if(conf.getBoolean("functional-tests.log")) "DEBUG" else "OFF"}
  )

  val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  try {
    val tableName = "dftest"
    val schema = new StructType(Array(StructField("col1", IntegerType)))

    val data = Seq(Row(77))
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
    println(df.toString())
    val mode = SaveMode.ErrorIfExists

    df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

  } finally {
    spark.close()
  }
   */
}
