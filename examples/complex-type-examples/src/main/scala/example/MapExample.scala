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
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI

object MapExample {
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
    } catch {
      case e: Exception => e.printStackTrace()
    }
    spark.close()
  }
}
