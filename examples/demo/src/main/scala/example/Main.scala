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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.sql.Connection
import scala.util.{Failure, Success, Try}

class Examples(conf: Config) {

  /**
   * Base options needed to connect to Vertica
   * */
  val options = Map(
    "host" -> conf.getString("functional-tests.host"),
    "user" -> conf.getString("functional-tests.user"),
    "db" -> conf.getString("functional-tests.db"),
    "staging_fs_url" -> conf.getString("functional-tests.filepath"),
    "password" -> conf.getString("functional-tests.password")
  )

  val conn: Connection = TestUtils.getJDBCConnection(options("host"), db = options("db"), user = options("user"), password = options("password"))

  val VERTICA_SOURCE = "com.vertica.spark.datasource.VerticaSource"

  private val spark = SparkSession.builder()
    .master("local[*]")
    .appName("Vertica Connector Test Prototype")
    .getOrCreate()

  private def printMessage(msg: String): Unit = println(s"------------------------------------\n-\n- EXAMPLE: $msg \n-\n------------------------------------")
  private def printSuccess(msg: String): Unit = println(s"------------------------------------\n-\n- SUCCESS: $msg \n-\n------------------------------------")
  private def printFailed(msg: String): Unit = println(s"-------------------------------------\n-\n- FAILED: $msg  \n-\n------------------------------------")

  def writeReadExample(): Unit = {

    printMessage("write data into Vertica then read it back")

    try {
      val tableName = "dftest"
      // Define schema of a table with a single integer attribute
      val schema = new StructType(Array(StructField("col1", IntegerType)))
      // Create n rows with element '77'
      val n = 20
      val data = (0 until n).map(_ => Row(77))
      // Create a dataframe corresponding to the schema and data specified above
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      // Outputs dataframe schema
      println(df.toString())
      // Save mode
      val mode = SaveMode.Overwrite
      // Write dataframe to Vertica
      df.write.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .mode(mode)
        .save()

      // Read data from Vertica a dataframe
      val dfRead = spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()

      dfRead.show()
    } finally {
      spark.close()
    }

    printMessage("Data written to Vertica")

  }

  def columnPushdown(): Unit = {
    // Aim of this case is to read in a table and project a specific column
    printMessage("Reading with column pushdown")

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
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      // Creates a new dataframe using only col b
      val dfCol = df.select("b")

      // Only prints values of col b, since that's what we selected
      dfCol.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }

    printMessage("SUCCESS")
  }

  def filterPushdown(): Unit = {
    // Aim of this case is to read in a table and filter based on value conditions
    printMessage("Reading with filter pushdown.")

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
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()

      // Create dataframes by filtering based on specific conditions
      printMessage("Showing data a > 4")
      val dfGreater = df.filter("a > 4")
      dfGreater.rdd.foreach(x => println("Read value " + x))

      printMessage("Showing data b == 1 and a > 8")
      val dfAnd = df.filter("b == 1 and a > 8")
      dfAnd.rdd.foreach(x => println("Read value " + x))

      printMessage("Showing data a = 2 or a > 8")
      val dfOr = df.filter("a = 2 or a > 8")
      dfOr.rdd.foreach(x => println("Read value " + x))

    } finally {
      spark.close()
      conn.close()
    }
  }

  def writeAppendMode(): Unit = {
    printMessage("Writing in append mode")
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
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val data = Seq(Row(77))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Append

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).mode(mode).save()
      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

      printSuccess("Data appended to Vertica")

    } finally {
      spark.close()
    }
  }

  def writeOverwriteMode(): Unit = {
    printMessage("Writing in overwrite mode")
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
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val schema = new StructType(Array(StructField("col1", StringType)))

      val data = Seq(Row("hello"), Row("world"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Overwrite
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).mode(mode).save()

      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

      printSuccess("Data overwritten into Vertica")
    } finally {
      spark.close()
    }
  }

  def writeErrorIfExistsMode(): Unit = {
    printMessage("Writing in error if exists mode")
    // ErrorIfExists mode means that when saving a DataFrame to a data source,
    // if data already exists, an exception is expected to be thrown.

    Try {
      val tableName = "dftest"
      val schema = new StructType(Array(StructField("col1", FloatType)))

      val data = (1 to 1000).map(x => Row(x.toFloat + 0.5f))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.ErrorIfExists

      // We expect an error to be thrown here.
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).mode(mode).save()

    } match {
      case Failure(exception) =>
        exception.printStackTrace()
        printSuccess("We should expects an error.")
      case Success(_) => printFailed("We should expects an error.")
    }

    spark.close()
  }

  def writeIgnoreMode(): Unit = {
    printMessage("Writing in ignore mode")
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
      val dfBefore: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS BEFORE: ")
      dfBefore.rdd.foreach(x => println("VALUE: " + x))

      val schema = new StructType(Array(StructField("col1", IntegerType)))

      printMessage("Writing in ignore mode")
      val data = (1 to 10000).map(x => Row(x))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Ignore

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).mode(mode).save()

      // Show table contents after the write
      val dfAfter: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      println("TABLE CONTENTS AFTER: ")
      dfAfter.rdd.foreach(x => println("VALUE: " + x))

      printSuccess("See above that the data did not change")

    } finally {
      spark.close()
    }
  }

  def writeCustomStatement(): Unit = {
    // Aim of this case is to use a custom statement to create a table, which
    // then gets written to using a dataframe

    printMessage("Writing with custom create table statement and copy list")

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

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options +
        ("table" -> tableName, "target_table_sql" -> customCreate)).mode(mode).save()

      printSuccess("Data written to Vertica. Check the log for the CREATE TABLE statement.")

    } finally {
      spark.close()
    }
  }

  def writeCustomCopyList(): Unit = {
    printMessage("Writing with custom create table statement and copy list")
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

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(options +
        ("table" -> tableName, "target_table_sql" -> customCreate, "copy_column_list" -> copyList)).mode(mode).save()

      printSuccess("Data written to Vertica")

    } finally {
      spark.close()
    }
  }

  /**
   * Native arrays are defined by Vertica as 1D arrays of primitive types only.
   * @see <a href="https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/DataTypes/ARRAY.htm">here</a>
   * */
  def nativeArrayExample(): Unit = {
    printMessage("Write native array into Vertica then read it back")
    // Define schema of a table with a 1D array column
    val schema = new StructType(Array(StructField("1D_array", ArrayType(IntegerType))))
    // Data
    val data = Seq(Row(Array(1, 1, 1, 2, 2, 2)))
    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

    try {
      val tableName = "1D_array"
      // Write dataframe to Vertica. note the data source
      val writeOpts = options + ("table" -> tableName)
      df.write.format(VERTICA_SOURCE)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      // Loading Vertica table
      spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .show()

      printSuccess("Data written to Vertica")

    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()
  }

  /**
   * Set is a special native array that contains no duplicate values.
   * */
  def setExample(): Unit = {
    printMessage("Write then set into Vertica then read it back")

    val tableName = "Set"
    // Marking the array as a Set.
    val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build()
    // Define schema of a table with a 1D array column
    val schema = new StructType(Array(StructField("Set", ArrayType(IntegerType), metadata = metadata)))
    // Data. Note the repeating numbers
    val data = Seq(Row(Array(1, 1, 1, 2, 2, 2)))
    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

    try {
      // Write dataframe to Vertica. note the data source
      val writeOpts = options + ("table" -> tableName)
      df.write.format(VERTICA_SOURCE)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      // Loading Vertica table
      spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .show()

      printSuccess("Data written to Vertica")

    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()
  }

  /**
   * Complex arrays are defined by Vertica as arrays which contains other complex types, include other
   * arrays.
   *
   * @see <a href="https://www.vertica.com/docs/11.0.x/HTML/Content/Authoring/SQLReferenceManual/DataTypes/ARRAY.htm">here</a>
   *
   * */
  def complexArrayExample(): Unit = {
    printMessage("Write then read complex array")

    // First, we need to define the schema of a table with a row type column.
    // This may be omitted if your dataframe already has schema info.
    // Vertica Row = SparkSQL StructType.
    val schema = new StructType(Array(
      // Complex type tables require at least one native type column
      StructField("native_array", ArrayType(IntegerType)),
      StructField("nested_array", ArrayType(ArrayType(IntegerType))),

      // Map type is not supported by Vertica.
      // It is suggested to use Array[Row] to represent map types instead.
      StructField("internal_map", ArrayType(
        StructType(Array(
          StructField("key", StringType),
          StructField("value", IntegerType),
        ))
      )),
    ))

    val data = Seq(Row(
      Array(12754),
      Array(Array(12754)),
      Array(
        Row(
          "key_1", 4812
        ),
        Row(
          "key_2", 3415
        )
      )
    ))

    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

    try {
      val tableName = "Complex_Array_Examples"
      // Write dataframe to Vertica. note the data source
      val writeOpts = options + ("table" -> tableName)
      df.write.format(VERTICA_SOURCE)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      // Loading Vertica table
      spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .show(false)

      printSuccess("Data written to Vertica")

    } catch {
      case e:Exception => e.printStackTrace()
    }

    spark.close()
  }

  def rowExample(): Unit = {
    printMessage("Write row into Vertica then it back.")

    // Define schema of a table with a row type column.
    // Vertica Row = SparkSQL StructType.
    val schema = new StructType(Array(
      // Complex type tables require at least one primitive type column
      StructField("required_primitive", ArrayType(IntegerType)),
      StructField("Row", StructType(Array(
        StructField("field1", StringType),
        // If field name is undefined, Vertica creates them.
        StructField("nested_array", ArrayType(ArrayType(DoubleType))),
        // Nested row
        StructField("inner_row", StructType(Array(
          StructField("field1", IntegerType)
        )))
      )))
    ))

    val data = Seq(Row(
      Array(12754),
      Row(
        "Vertica",
        Array(Array(4.5, 1.2, 6.7, 4.0)),
        Row(
          90
        )
      )
    ))

    // Create a dataframe corresponding to the schema and data specified above
    val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

    try {
      val tableName = "Row_Example"
      // Write dataframe to Vertica. note the data source
      val writeOpts = options + ("table" -> tableName)
      df.write.format(VERTICA_SOURCE)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      // Loading Vertica table
      spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .show(false)

      printSuccess("Data written to Vertica")

    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()

  }

  def mapExample(): Unit = {
    printMessage("Write map to Vertica as an external table, then read it")
    try {
      // Ensure that the external data location is clear
      val fs = FileSystem.get(new URI(conf.getString("functional-tests.filepath")), new Configuration())
      val externalDataLocation = new Path("/data")
      fs.delete(externalDataLocation, true)

      val tableName = "dftest"
      val schema = new StructType(Array(
        StructField("col2", MapType(StringType, IntegerType))
      ))

      val data = Seq(
        Row(Map("key" -> 1))
      )

      val writeOpts = options + (
        "table" -> tableName,
        "create_external_table" -> "true",
        "staging_fs_url" -> (conf.getString("functional-tests.filepath") + "external_data")
      )

      // Write to Vertica an external table with Map type
      spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
        .write
        .options(writeOpts)
        .format(VERTICA_SOURCE)
        .mode(SaveMode.Overwrite)
        .save()

      printSuccess("Map written to external table. Due to Vertica's limitations, we cannot read it from Vertica and can only write them to external tables.\n" +
       "More information here: https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/DataTypes/MAP.htm")
    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()
  }
}

object Main {
  def main(args: Array[String]): Unit = {
    def noCase(): Unit = println("EXAMPLE: No case defined with that name")

    val conf: Config = ConfigFactory.load()

    val examples = new Examples(conf)

    val m: Map[String, () => Unit] = Map(
      "columnPushdown" -> examples.columnPushdown,
      "filterPushdown" -> examples.filterPushdown,
      "writeAppendMode" -> examples.writeAppendMode,
      "writeOverwriteMode" -> examples.writeOverwriteMode,
      "writeErrorIfExistsMode" -> examples.writeErrorIfExistsMode,
      "writeIgnoreMode" -> examples.writeIgnoreMode,
      "writeCustomStatement" -> examples.writeCustomStatement,
      "writeCustomCopyList" -> examples.writeCustomCopyList,
      "writeReadExample" -> examples.writeReadExample,
      "complexArrayExample" -> examples.complexArrayExample,
      "rowExample" -> examples.rowExample,
      "mapExample" -> examples.mapExample
    )

    if (args.length != 1) {
      println("EXAMPLE: Please enter name of demo case to run.")
    }
    else {
      val f: () => Unit = m.getOrElse(args.head, noCase)
      try{
        f()
      }
      catch {
        case e: Exception => e.printStackTrace()
      }
    }
  }
}
