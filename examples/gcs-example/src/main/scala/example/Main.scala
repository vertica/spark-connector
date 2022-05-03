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

import com.typesafe.config.ConfigFactory
import com.typesafe.config.Config
import org.apache.spark.sql.types.{IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

import scala.util.{Failure, Success, Try}

object Main {
  def main(args: Array[String]): Unit = {
    val conf: Config = ConfigFactory.load()

    // Configuration options for the connector
    var options = Map(
      "host" -> conf.getString("app.host"),
      "db" -> conf.getString("app.db"),
      "user" -> conf.getString("app.db_user"),
      "password" -> conf.getString("app.db_password"),
      // Loading service account HMAC key. Required for GCS access
      "gcs_hmac_key_id" -> conf.getString("app.hmac_key_id"),
      "gcs_hmac_key_secret" -> conf.getString("app.hmac_key_secret"),
      // Your GCS bucket address
      "staging_fs_url" -> conf.getString("app.gcs_bucket"),
    )

    // Loading keyfile path. Only required if running outside of GCP cluster.
    Try{conf.getString("app.keyfile_path")} match {
      case Success(path) => options = options + ("gcs_keyfile" -> path)
      case Failure(_) => ()
    }

    // Initialize Spark
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica Connector Test Prototype")
      .getOrCreate()

    val verticaSource = "com.vertica.spark.datasource.VerticaSource"
    try {
      val tableName = "dftest"
      val rdd = spark.sparkContext.parallelize(Seq(
        Row(23),
        Row(35),
        Row(75),
        Row(96)
      )).coalesce(1)
      val schema = StructType(Array(StructField("col1", IntegerType)))
      val writeOpts = options + ("table" -> tableName)
      spark.createDataFrame(rdd, schema)
        .write.format(verticaSource)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      val readOpts = options + ("table" -> tableName)
      spark.read.format(verticaSource)
        .options(readOpts)
        .load()
        .show()
    } finally {
      spark.close()
    }
  }
}