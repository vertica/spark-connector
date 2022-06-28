// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

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

    // Creating a Spark context
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val VERTICA_SOURCE = "com.vertica.spark.datasource.VerticaSource"

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

      // Read dftest into a dataframe
     val dfRead = spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()

      dfRead.show()

    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      spark.close()
    }
  }
}
