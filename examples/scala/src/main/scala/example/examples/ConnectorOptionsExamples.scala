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

package example.examples

import com.typesafe.config.{Config, ConfigFactory}
import example.PrintUtils.{printMessage, printNotes, printSuccess}
import example.TestUtils
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

import java.sql.Connection

class ConnectorOptionsExamples(spark: SparkSession) {
  val conf: Config = ConfigFactory.load()
  /**
   * Base options needed to connect to Vertica
   * */
  val options = Map(
    "host" -> conf.getString("examples.host"),
    "user" -> conf.getString("examples.user"),
    "db" -> conf.getString("examples.db"),
    "staging_fs_url" -> conf.getString("examples.filepath"),
    "password" -> conf.getString("examples.password")
  )

  val conn: Connection = TestUtils.getJDBCConnection(options("host"), db = options("db"), user = options("user"), password = options("password"))

  val VERTICA_SOURCE = "com.vertica.spark.datasource.VerticaSource"



  /**
   * The connector supports writing to Vertica using a custom create table statement through the
   * option `target_table_sql`. When the connector needs to create a table, it will use the CREATE
   * TABLE statement defined in `target_table_sql` instead.
   * */
  def writeCustomStatement(): Unit = {

    printMessage("Writing with custom create table statement and copy list")

    try {
      val tableName = "dftest"
      // The schema of our data. The connector will analyze the schema and create an appropriate CREATE TABLE
      // statement with 2 columns.
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType)))

      // However, using `target_table_sql` we can overwrite the CREATE TABLE statement with our own CREATE TABLE
      // statement which added a third column.
      val customCreate = "CREATE TABLE dftest(col1 integer, col2 varchar(2345), col3 float);"

      val data = (1 to 1000).map(x => Row(x, "test"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())

      // Using Overwrite mode guarantees we always create a new table.
      val mode = SaveMode.Overwrite
      df.write.format(VERTICA_SOURCE).options(
        options +
          ("table" -> tableName,
            "target_table_sql" -> customCreate)
      ).mode(mode).save()

      printSuccess("Data written to Vertica. Check VERTICA for table's schema")

    } finally {
      spark.close()
    }
  }

  /**
   * The `copy_column_list` allows users to specify a list of columns for the COPY statement when writing data to Vertica.
   * */
  def writeCustomCopyList(): Unit = {

    printMessage("Writing with custom create table statement and copy list")

    try {
      val tableName = "dftest"
      // Creating a table in Vertica using a custom create statement.
      val customCreate = "CREATE TABLE dftest(a integer, b varchar(2345), c integer);"

      // The schema for our Spark dataframe. This will cause an error when the COPY statement is executed since
      // the column names are different from our custom create statement.
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", StringType)))

      // Thus, we need to define the column names for COPY using copy_column_list
      val copyList = "a, b"

      val data = (1 to 1000).map(x => Row(x, "test"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      println(df.toString())
      val mode = SaveMode.Overwrite

      df.write.format(VERTICA_SOURCE).options(
        options +
          ("table" -> tableName,
            "target_table_sql" -> customCreate,
            "copy_column_list" -> copyList))
        .mode(mode).save()

      printSuccess("Data written to Vertica")

    } finally {
      spark.close()
    }
  }

  /**
   * The connector can also merge dataframes into existing Vertica table using the option `merge_key`.
   * `merge_key` is a comma separated list of column names to be merged.
   * @see <a href="https://www.vertica.com/docs/latest/HTML/Content/Authoring/AdministratorsGuide/Tables/MergeTables/SynchronizingTableDataWithMerge.htm"> docs <a/>
   * */
  def writeDataUsingMergeKey() : Unit = {

    printMessage("Merging data into an existing table in Vertica, then read it back.")

    try {
      val tableName = "test-data"
      val schema = new StructType(Array(StructField("col1", IntegerType), StructField("col2", IntegerType), StructField("col3", StringType), StructField("col4", StringType)))
      val data = (1 to 5).map(x => Row(x, 3, "cat", "shark"))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

      // Write the table into Vertica
      df.write.format(VERTICA_SOURCE)
        .mode(SaveMode.Overwrite)
        .options(options + ("table" -> tableName))
        .save()

      val preMergedTable = spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .cache()

      // Create a new dataframe
      val data2 = (1 to 5).map(x => Row(3, x, "shark", "cat"))
      val df2 = spark.createDataFrame(spark.sparkContext.parallelize(data2), schema).coalesce(1)

      // Merge the new dataframe into the previous created table in Vertica on these columns
      val mergeKeys = "col1, col2"
      df2.write.format(VERTICA_SOURCE)
        .options(
          options +
            ("table" -> tableName) +
            ("merge_key" -> mergeKeys))
        .mode(SaveMode.Overwrite)
        .save()

      val mergedTable = spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()
        .cache()

      preMergedTable.show()
      printNotes("Pre-Merged Data. Notes col1=3, col2=3")
      mergedTable.show()
      printNotes("Merged Data. We expects col1=3, col2=3 to be overwritten with the new data")
    } finally {
      spark.close()
    }
  }
}
