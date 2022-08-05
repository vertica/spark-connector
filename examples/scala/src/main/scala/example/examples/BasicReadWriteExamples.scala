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
import example.PrintUtils.{printMessage, printSuccess}
import example.TestUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.sql.Connection

class BasicReadWriteExamples(spark: SparkSession) {
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
   * A simple example demonstrating how to write into Vertica, then read back the data in a dataframe.
   * */
  def writeThenReadHDFS(): Unit = {

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

      /**
       * The connector supports different Spark modes to handle writing to existing table:
       * - Append: Data is appended to the existing table.
       * - Overwrite: Data will overwrite the existing table.
       * - ErrorIfExists: Throw an [[TableAlreadyExistsException]] exception if table already exists.
       * - Ignore: Do nothing if data already exists.
       * */
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

  /**
   * This example show how to write a dataframe as an external table.
   * */
  def createExternalTable(): Unit = {

    printMessage("Create an external table and write data to it, then read it back.")

    try {
      val tableName = "existingData"
      val filePath = options("staging_fs_url") + "existingData"

      // HDFS preparations: deleting the staging folder to ensure that the external data location is clear
      val fs = FileSystem.get(new URI(options("staging_fs_url")), new Configuration())
      val externalDataLocation = new Path("/data")
      fs.delete(externalDataLocation, true)

      val input1 = Array.fill[Byte](100)(0)
      val input2 = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx8"
      val data = Seq(Row(input1, input2))
      // Create "existing data" on disk
      val schema = new StructType(Array(StructField("col1", BinaryType), StructField("col2", StringType)))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      df.write.parquet(filePath)

      // Write an empty dataframe using our connector to create an external table out of existing data
      val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], new StructType())
      val mode = SaveMode.Overwrite
      df2.write.format(VERTICA_SOURCE)
        .options(
          options +
            ("staging_fs_url" -> filePath,
              "table" -> tableName,
              "create_external_table" -> "existing-data"))
        .mode(mode)
        .save()

      val readDf: DataFrame = spark.read.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .load()

      readDf.show()
      printSuccess("Data written as an external table.")

    } finally {
      spark.close()
    }
  }


  /**
   * Example show write and read using Amazon S3.
   * */
  def writeThenReadWithS3(): Unit = {

    printMessage("Writing to Vertica using S3, then reading it back.")

    // Adding S3 auth credentials and settings to connector the our S3 minio container.
    // Refer to our README for a more available settings.
    val optionsS3 = options - ("staging_fs_url") + (
      "staging_fs_url" -> conf.getString("s3.filepath"),
      "aws_access_key_id" -> conf.getString("s3.aws_access_key_id"),
      "aws_secret_access_key" -> conf.getString("s3.aws_secret_access_key"),
      "aws_endpoint" -> conf.getString("s3.aws_endpoint"),
      "aws_enable_ssl" -> conf.getString("s3.aws_enable_ssl"),
      "aws_enable_path_style" -> conf.getString("s3.aws_enable_path_style"),
    )

    try {
      val tableName = "dftest"
      // Define schema of a table with a single integer attribute
      val schema = new StructType(Array(StructField("col1", IntegerType)))
      // Create n rows with element '77'
      val n = 20
      val data = (0 until n).map(_ => Row(77))
      // Create a dataframe corresponding to the schema and data specified above
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Overwrite

      // Write dataframe to Vertica with S3
      df.write.format(VERTICA_SOURCE)
        .options(optionsS3 + ("table" -> tableName))
        .mode(mode)
        .save()

      // Read dataframe to Vertica with S3
      val dfRead: DataFrame = spark.read.format(VERTICA_SOURCE)
        .options(optionsS3 + ("table" -> tableName))
        .load()

      dfRead.show()

      printSuccess("Data write/read from Vertica using S3.")

    } finally {
      spark.close()
      conn.close()
    }
  }

  /**
   * Example show how to configure for using with GCS. You will need to bring your own GCS credentials.
   * */
  def writeThenReadWithGCS(): Unit = {

    printMessage("Write data to Vertica using GCS, then read it back.")

    // Enter your GCS auth options below before starting the example.
    val optionsGCS = options - ("staging_fs_url") + (
      "staging_fs_url" -> "Your GCS bucket path",
      "gcs_hmac_key_id" -> "Your GCS HMAC key id here",
      "gcs_hmac_key_secret" -> "Your GCS HMAC key secret here",
      "gcs_service_keyfile" -> "The path to your GCS service keyfile json here",
    )

    try {
      val tableName = "dftest"
      val rdd = spark.sparkContext.parallelize(Seq(
        Row(23),
        Row(35),
        Row(75),
        Row(96)
      )).coalesce(1)

      val schema = StructType(Array(StructField("col1", IntegerType)))
      val writeOpts = optionsGCS + ("table" -> tableName)
      spark.createDataFrame(rdd, schema)
        .write.format(VERTICA_SOURCE)
        .options(writeOpts)
        .mode(SaveMode.Overwrite)
        .save()

      val readOpts = optionsGCS + ("table" -> tableName)
      spark.read.format(VERTICA_SOURCE)
        .options(readOpts)
        .load()
        .show()

      printMessage("Data written/read back using GCS.")
    } finally {
      spark.close()
    }
  }

  /**
   * Example demonstrate how to configure the connector for Kerberos. We provide a docker environment
   * */
  def writeThenReadWithKerberos(): Unit = {

    printMessage("Writing to Vertica with Kerberos authentication, then read it back")

    val optionsKerberos = options - ("filepath") - ("user") + (
      "user" -> conf.getString("kerberos.user"),
      "filepath" -> conf.getString("kerberos.filepath"),
      "kerberos_service_name" -> conf.getString("kerberos.kerberos_service_name"),
      "kerberos_host_name" -> conf.getString("kerberos.kerberos_host_name"),
      "jaas_config_name" -> conf.getString("kerberos.jaas_config_name"))

    try {
      val tableName = "test"
      val schema = new StructType(Array(StructField("col1", IntegerType)))

      val data = Seq.iterate(0, 1000)(_ + 1).map(x => Row(x))
      val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)
      val mode = SaveMode.Overwrite

      df.write.format(VERTICA_SOURCE)
        .options(optionsKerberos + ("table" -> tableName))
        .mode(mode).save()
      println("KERBEROS DEMO, WROTE TABLE")

      printSuccess("Data written to Vertica")

      val dfRead: DataFrame = spark.read
        .format(VERTICA_SOURCE)
        .options(optionsKerberos + ("table" -> tableName))
        .load()

      dfRead.show()

      printSuccess("Data loaded back")

    } finally {
      spark.close()
    }
  }
}
