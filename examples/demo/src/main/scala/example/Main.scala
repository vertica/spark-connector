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
import org.apache.spark.sql.types.{FloatType, IntegerType, StringType, StructField, StructType}
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

  val writeOpts = Map(
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
      val tableName = "readtest"
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
      val tableName = "readtest"
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
      val tableName = "readtest"
      val stmt = conn.createStatement
      val n = 20
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")

      val insert = "insert into " + tableName + " values(2, 3)"
      TestUtils.populateTableBySQL(stmt, insert, n)
      val insert2 = "insert into " + tableName + " values(5, 1)"
      TestUtils.populateTableBySQL(stmt, insert2, n)

      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      val dfGreater = df.filter("a > 4")
      dfGreater.rdd.foreach(x => println("DEMO: Read value " + x))

      val dfEqual = df.filter("b == 1")
      dfEqual.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }

  def writeAppendMode(): Unit = {
    println("DEMO: Writing in append mode")

    try {
      val tableName = "dftest"
      val schema = new StructType(Array(StructField("col1", IntegerType)))

      val data = Seq(Row(77))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Append

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    } finally {
      spark.close()
    }
  }

  def writeOverwriteMode(): Unit = {
    println("DEMO: Writing in overwrite mode")

    try {
      val tableName = "dftest"
      val schema = new StructType(Array(StructField("col1", StringType)))

      val data = Seq(Row("hello"), Row("world"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Overwrite

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    } finally {
      spark.close()
    }
  }

  def writeErrorIfExistsMode(): Unit = {
    println("DEMO: Writing in error if exists mode")

    try {
      val tableName = "dftest"
      val schema = new StructType(Array(StructField("col1", FloatType)))

      val data = (1 to 1000).map(x => Row(x.toFloat + 0.5f))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.ErrorIfExists

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    } finally {
      spark.close()
    }
  }

  def writeIgnoreMode(): Unit = {
    println("DEMO: Writing in ignore mode")

    try {
      val tableName = "dftest"
      val schema = new StructType(Array(StructField("col1", IntegerType)))

      val data = (1 to 10000).map(x => Row(x))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Ignore

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

    } finally {
      spark.close()
    }
  }

  def writeCustomStatement(): Unit = {
    println("DEMO: Writing with custom create table statement and copy list")

    try {
      val tableName = "dftest"
      val customCreate = "CREATE TABLE dftest(col1 integer, col2 string, col3 float);"
      val copyList = "col1, col2"
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType)))

      val data = (1 to 1000).map(x => Row(x, "test"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Ignore

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts +
        ("table" -> tableName, "target_table_sql" -> customCreate, "copy_column_list" -> copyList)).mode(mode).save()

    } finally {
      spark.close()
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
    "filterPushdown" -> demoCases.filterPushdown,
    "writeAppendMode" -> demoCases.writeAppendMode,
    "writeOverwriteMode" -> demoCases.writeOverwriteMode,
    "writeErrorIfExistsMode" -> demoCases.writeErrorIfExistsMode,
    "writeIgnoreMode" -> demoCases.writeIgnoreMode,
    "writeCustomStatement" -> demoCases.writeCustomStatement
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
