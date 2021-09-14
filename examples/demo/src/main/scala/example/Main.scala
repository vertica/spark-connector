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
    "password" -> conf.getString("functional-tests.password")
  )

  val writeOpts = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password")
  )

  val conn: Connection = TestUtils.getJDBCConnection(readOpts("host"), db = readOpts("db"), user = readOpts("user"), password = readOpts("password"))

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()


  def columnPushdown(): Unit = {
    // Aim of this case is to read in a table and project a specific column
    println("DEMO: Reading with column pushdown.")

    try {
      val tableName = "readtest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called readtest with two integer attributes
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")

      val insert = "insert into " + tableName + " values(2, 3)"
      // Inserts 20 rows of values 2 and 3 in cols a and b, respectively
      TestUtils.populateTableBySQL(stmt, insert, n)

      // Reads readtest into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      // Creates a new dataframe using only col b
      val dfCol = df.select("b")

      // Only prints values of col b, since that's what we selected
      dfCol.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }
  }

  def filterPushdown(): Unit = {
    // Aim of this case is to read in a table and filter based on value conditions
    println("DEMO: Reading with filter pushdown.")

    try {
      val tableName = "readtest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called readtest in Vertica with two integer attributes, a and b
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")
      // Inserts 80 rows into readtest
      val insert = "insert into " + tableName + " values(2, 3)"
      TestUtils.populateTableBySQL(stmt, insert, n)
      val insert2 = "insert into " + tableName + " values(5, 1)"
      TestUtils.populateTableBySQL(stmt, insert2, n)
      val insert3 = "insert into " + tableName + " values(10, 1)"
      TestUtils.populateTableBySQL(stmt, insert3, n)
      val insert4 = "insert into " + tableName + " values(-10, 0)"
      TestUtils.populateTableBySQL(stmt, insert4, n)
      // Read the newly created table into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()

      // Create dataframes by filtering based on specific conditions
      val dfGreater = df.filter("a > 4")
      dfGreater.rdd.foreach(x => println("DEMO: Read value " + x))

      val dfAnd = df.filter("b == 1 and a > 8")
      dfAnd.rdd.foreach(x => println("DEMO: Read value " + x))

      val dfOr = df.filter("a = 2 or a > 8")
      dfOr.rdd.foreach(x => println("DEMO: Read value " + x))

    } finally {
      spark.close()
      conn.close()
    }
  }

  def writeAppendMode(): Unit = {
    println("DEMO: Writing in append mode")
    // Append mode means that when saving a DataFrame to a data source,
    // if data/table already exists, contents of the DataFrame are expected
    // to be appended to existing data.

    try {
      // Create a table called dftest in Vertica and populate it
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 10
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (col1 int)")

      val insert = "insert into " + tableName + " values(2)"
      TestUtils.populateTableBySQL(stmt, insert, n)

      val schema = new StructType(Array(StructField("col1", IntegerType)))
      // Show table contents before the write
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val data = Seq(Row(77))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Append

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()
      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

    } finally {
      spark.close()
    }
  }

  def writeOverwriteMode(): Unit = {
    println("DEMO: Writing in overwrite mode")
    // Overwrite mode means that when saving a DataFrame to a data source, if data/table already exists,
    // existing data is expected to be overwritten by the contents of the DataFrame.

    try {
      // Create a table called dftest in Vertica and populate it
      val tableName = "dftest"
      val stmt = conn.createStatement
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (col1 varchar)")

      val insert = "insert into " + tableName + " values('Hola')"
      TestUtils.populateTableBySQL(stmt, insert, 1)
      // Show table contents before the write
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val schema = new StructType(Array(StructField("col1", StringType)))

      val data = Seq(Row("hello"), Row("world"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Overwrite
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

    } finally {
      spark.close()
    }
  }

  def writeErrorIfExistsMode(): Unit = {
    println("DEMO: Writing in error if exists mode")
    // ErrorIfExists mode means that when saving a DataFrame to a data source,
    // if data already exists, an exception is expected to be thrown.

    // This example should output an error
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
    // Ignore mode means that when saving a DataFrame to a data source, if data
    // already exists, the save operation is expected to not save the contents of
    // the DataFrame and to not change the existing data.

    try {
      val tableName = "dftest"
      val stmt = conn.createStatement
      val n = 10
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (col1 int)")

      val insert = "insert into " + tableName + " values(7)"
      TestUtils.populateTableBySQL(stmt, insert, n)

      // Show table contents before the write
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val schema = new StructType(Array(StructField("col1", IntegerType)))

      val data = (1 to 10000).map(x => Row(x))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Ignore

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("table" -> tableName)).mode(mode).save()

      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(readOpts + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

    } finally {
      spark.close()
    }
  }

  def writeCustomStatement(): Unit = {
    // Aim of this case is to use a custom statement to create a table, which
    // then gets written to using a dataframe

    println("DEMO: Writing with custom create table statement and copy list")

    try {
      val tableName = "dftest"
      val customCreate = "CREATE TABLE dftest(col1 integer, col2 varchar(2345), col3 float);"
      // Schema has a subset of the same col names as our custom statement
      // When writing to dftest in Vertica, it will match the cols
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType)))

      val data = (1 to 1000).map(x => Row(x, "test"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Overwrite

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts +
        ("table" -> tableName, "target_table_sql" -> customCreate)).mode(mode).save()

    } finally {
      spark.close()
    }
  }

  def writeCustomCopyList(): Unit = {
    println("DEMO: Writing with custom create table statement and copy list")
    // Similar to the case above, this use-case will use a custom statement to create
    // a target table, where contents of the dataframe will be copied
    try {
      val tableName = "dftest"
      val customCreate = "CREATE TABLE dftest(a integer, b varchar(2345), c integer);"
      // Use copyList paramater to specify which cols to write to in dftest
      val copyList = "a, b"
      // Schema has different col names than the customCreate statement, which is why we need copyList
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType)))

      val data = (1 to 1000).map(x => Row(x, "test"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Overwrite

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts +
        ("table" -> tableName, "target_table_sql" -> customCreate, "copy_column_list" -> copyList)).mode(mode).save()

    } finally {
      spark.close()
    }
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    def noCase(): Unit = println("DEMO: No case defined with that name")

    val conf: Config = ConfigFactory.load()

    val demoCases = new DemoCases(conf)

    val m: Map[String, () => Unit] = Map(
      "columnPushdown" -> demoCases.columnPushdown,
      "filterPushdown" -> demoCases.filterPushdown,
      "writeAppendMode" -> demoCases.writeAppendMode,
      "writeOverwriteMode" -> demoCases.writeOverwriteMode,
      "writeErrorIfExistsMode" -> demoCases.writeErrorIfExistsMode,
      "writeIgnoreMode" -> demoCases.writeIgnoreMode,
      "writeCustomStatement" -> demoCases.writeCustomStatement,
      "writeCustomCopyList" -> demoCases.writeCustomCopyList
    )

    if(args.length != 1) {
      println("DEMO: Please enter name of demo case to run.")
    }
    else {
      val f: () => Unit = m.getOrElse(args.head, noCase)
      f()
    }
  }
}
