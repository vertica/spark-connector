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

    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val VERTICA_SOURCE = "com.vertica.spark.datasource.VerticaSource"

    var dataframes = List[DataFrame]()

    try {
      dataframes = dataframes :+ {
        /** ******************************
         *
         * Array Examples
         *
         * ****************************** */
        val tableName = "1D_array"
        // Define schema of a table with a 1D array column
        val schema = new StructType(Array(StructField("1D_array", ArrayType(IntegerType))))
        // Data
        val data = Seq(Row(Array(1, 1, 1, 2, 2, 2)))
        // Create a dataframe corresponding to the schema and data specified above
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
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
      }

      dataframes = dataframes :+ {
        /** ******************************
         *
         * Set Examples
         *
         * ****************************** */
        val tableName = "Set"
        // Marking the array as a Set.
        val metadata = new MetadataBuilder().putBoolean(MetadataKey.IS_VERTICA_SET, true).build()
        // Define schema of a table with a 1D array column
        val schema = new StructType(Array(StructField("Set", ArrayType(IntegerType), metadata = metadata)))
        // Data. Note the repeating numbers
        val data = Seq(Row(Array(1, 1, 1, 2, 2, 2)))
        // Create a dataframe corresponding to the schema and data specified above
        val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
        // Write dataframe to Vertica. note the data source
        val writeOpts = options + ("table" -> tableName)
        df.write.format(VERTICA_SOURCE)
          .options(writeOpts)
          // With overwrite mode, the connector will recreate a table with a set columns
          .mode(SaveMode.Overwrite)
          .save()

        // Loading Vertica table
        // The column will be of ArrayType with the metadata marked.
        spark.read.format(VERTICA_SOURCE)
          .options(options + ("table" -> tableName))
          .load()
      }

      dataframes = dataframes :+ {
        /** ******************************
         *
         * Complex Array Examples
         *
         * ****************************** */
        val tableName = "Complex_Array_Examples"
        // Define schema of a table with a row type column.
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
      }

      dataframes = dataframes :+ {
        /** ******************************
         *
         * Row Examples
         *
         * ****************************** */
        val tableName = "Row_Example"
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
      }

      {
        /** ******************************
         *
         * Writing an external table with Map type column
         *
         * ****************************** */


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

        // Map type cannot be queried in Vertica and is only allowed in external tables.
        // As such, the connector cannot read those tables.
        // Map type currently exists for interoperability with external data.
      }

    } catch {
      case e: Exception => e.printStackTrace()
    }

    dataframes.foreach(_.show(truncate = false))
    spark.close()
  }

}
