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
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

object RowExample {

  def main(Args: Array[String]): Unit = {

    val conf: Config = ConfigFactory.load()
    // Configuration options for the connector
    val options = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password")
    )

    // Creating a Spark context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val VERTICA_SOURCE = "com.vertica.spark.datasource.VerticaSource"

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
        .show(false)
    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()
  }

}
