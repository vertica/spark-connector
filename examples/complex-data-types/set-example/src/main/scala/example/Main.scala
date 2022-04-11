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

    try {
      val tableName = "dftest"
      // Table name needs to be specified in option
      val writeOpts = options + ("table" -> tableName)
      // Schema's metadata.
      val metadata = new MetadataBuilder()
        .putBoolean(MetadataKey.IS_VERTICA_SET, true)
        .build
      // Define schema of a table with a SET column. We add the metadata above to mark it as a Vertica SET.
      val schema = new StructType(Array(StructField("col1", ArrayType(IntegerType), metadata = metadata)))
      // Data. Note that unique elements will not be checked until Vertica starts ingesting.
      val data = Seq(Row(Array(1, 2, 3, 4, 5, 6)))
      // Create a dataframe corresponding to the schema and data specified above
      val dfWrite = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      // Write dataframe to Vertica. note the data source
      dfWrite.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(writeOpts)
        // In Overwrite mode, the table in Vertica will be dropped and recreated with a SET column
        .mode(SaveMode.Overwrite)
        .save()

      val readOpts = options + ("table" -> tableName)
      // Load a set from Vertica as array and display it
      val dfRead = spark.read.format("com.vertica.spark.datasource.VerticaSource")
        .options(readOpts)
        .load()
      dfRead.show()
      val col1 = dfRead.schema.fields(0)
      // When reading, an array will be marked if it is a set in Vertica.
      println(s"Is ${col1.name} SET type: " + col1.metadata.getBoolean(MetadataKey.IS_VERTICA_SET))
      println()
    } finally {
      spark.close()
    }
  }
}
