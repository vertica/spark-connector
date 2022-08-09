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
import com.vertica.spark.util.schema.MetadataKey
import example.PrintUtils.{printMessage, printSuccess}
import example.TestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, MapType, MetadataBuilder, StringType, StructField, StructType}

import java.net.URI
import java.sql.Connection

class ComplexTypeExamples(spark: SparkSession) {
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
   * Native arrays are defined by Vertica as 1D arrays of primitive types only.
   *
   * @see <a href="https://www.vertica.com/docs/latest/HTML/Content/Authoring/SQLReferenceManual/DataTypes/ARRAY.htm">here</a>
   * */
  def writeThenReadNativeArray(): Unit = {
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
  def writeThenReadSet(): Unit = {
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
  def writeThenReadComplexArray(): Unit = {
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
      case e: Exception => e.printStackTrace()
    }

    spark.close()
  }

  def writeThenReadRow(): Unit = {
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

  def writeMap(): Unit = {
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
