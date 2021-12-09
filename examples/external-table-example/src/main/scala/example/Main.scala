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
import org.apache.spark.sql.types.{IntegerType, StructField, StructType, FloatType, StringType, MetadataBuilder}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}

object Main  {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()
    // Configuration options for the connector
    val writeOpts = Map(
      "host" -> conf.getString("functional-tests.host"),
      "user" -> conf.getString("functional-tests.user"),
      "db" -> conf.getString("functional-tests.db"),
      "staging_fs_url" -> conf.getString("functional-tests.filepath"),
      "password" -> conf.getString("functional-tests.password"),
      "create_external_table" -> conf.getString("functional-tests.external")
    )
    // Entry-point to all functionality in Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    try {
      val tableName = "existingData"
      val filePath = writeOpts("staging_fs_url") + "existingData"

      val data = (1 to 20).map(x => Row("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx" + x.toString))
      val schema = new StructType(Array(StructField("col1", StringType)))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      df.write.parquet(filePath)

      val columnLengthMap = Map(
        "col1" -> 256
      )
      var df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)

      columnLengthMap.foreach { case (colName, length) =>
        val metadata = new MetadataBuilder().putLong("maxlength", length).build()
        df2 = df2.withColumn(colName, df2(colName).as(colName, metadata))
      }


      val mode = SaveMode.Overwrite
      df2.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("staging_fs_url" -> filePath, "table" -> tableName, "create_external_table" -> "existing-data")).mode(mode).save()


    } finally {
      spark.close()
    }
  }
}
