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

import com.typesafe.config.Config
import com.vertica.spark.util.schema.MetadataKey
import example.PrintUtils.{printMessage, printNotes, printSuccess}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.{DataFrame, Row, SaveMode, SparkSession}
import org.apache.spark.sql.types._

import java.net.URI
import java.sql.Connection

class Examples(conf: Config, spark: SparkSession) {

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
  def writeReadExample(): Unit = {

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
   * This example show how columns are pushed down to queries to Vertica when reading data.
   * To see this, the log output records the EXPORT statement used should only query for
   * column "b".
   * */
  def columnPushdown(): Unit = {
    printMessage("Reading with column pushdown")

    try {
      val tableName = "readtest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called readtest with two integer attributes
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")

      val insert = "insert into " + tableName + " values(2, 3)"
      // Inserts 20 rows of values 2 and 3 in cols a and b, respectively
      TestUtils.populateTableBySQL(stmt, insert, n)

      // Reads readtest into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()
      // Creates a new dataframe using only col b
      val dfCol = df.select("b")

      // Only prints values of col b, since that's what we selected
      dfCol.rdd.foreach(x => println("DEMO: Read value " + x))
    } finally {
      spark.close()
      conn.close()
    }

    printMessage("SUCCESS")
  }

  /**
   * This example shows how filters (conditions) are pushed down onto queries to Vertica.
   * The log output records the 3 EXPORT statements used which should now contains the conditions.
   * */
  def filterPushdown(): Unit = {
    printMessage("Reading with filter pushdown.")

    try {
      val tableName = "readtest"
      val stmt = conn.createStatement
      val n = 20
      // Creates a table called readtest in Vertica with two integer attributes, a and b
      TestUtils.createTableBySQL(conn, tableName, "create table " + tableName + " (a int, b int)")
      // Inserts 80 rows into readtest
      val insert = "insert into " + tableName + " values(2, 3)"
      TestUtils.populateTableBySQL(stmt, insert, n)
      val insert2 = "insert into " + tableName + " values(5, 1)"
      TestUtils.populateTableBySQL(stmt, insert2, n)
      val insert3 = "insert into " + tableName + " values(10, 1)"
      TestUtils.populateTableBySQL(stmt, insert3, n)
      val insert4 = "insert into " + tableName + " values(-10, 0)"
      TestUtils.populateTableBySQL(stmt, insert4, n)
      // Read the newly created table into a dataframe
      val df: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(options + ("table" -> tableName)).load()

      // Create dataframes by filtering based on specific conditions
      printMessage("Query for a > 4")
      val dfGreater = df.filter("a > 4")
      dfGreater.rdd.foreach(x => println("Read value " + x))

      printMessage("Query for b == 1 and a > 8")
      val dfAnd = df.filter("b == 1 and a > 8")
      dfAnd.rdd.foreach(x => println("Read value " + x))

      printMessage("Query for a = 2 or a > 8")
      val dfOr = df.filter("a = 2 or a > 8")
      dfOr.rdd.foreach(x => println("Read value " + x))

    } finally {
      spark.close()
      conn.close()
    }
  }

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
      df.write.format("com.vertica.spark.datasource.VerticaSource").options(
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

      df.write.format("com.vertica.spark.datasource.VerticaSource").options(
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
      df2.write.format("com.vertica.spark.datasource.VerticaSource")
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
      val dfRead: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource")
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

    // Enter your GCS auth options below or into the configuration file.
    val optionsGCS = options - ("staging_fs_url") + (
      // Loading service account HMAC key. Required for GCS access
      "gcs_hmac_key_id" -> conf.getString("gcs.gcs_hmac_key_id"),
      "gcs_hmac_key_secret" -> conf.getString("gcs.gcs_hmac_key_secret"),
      // Path to your keyfile.json
      "gcs_service_keyfile" -> conf.getString("gcs.gcs_service_keyfile"),
      // Your GCS bucket address
      "staging_fs_url" -> conf.getString("gcs.filepath"),
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

      df.write.format("com.vertica.spark.datasource.VerticaSource")
        .options(optionsKerberos + ("table" -> tableName))
        .mode(mode).save()
      println("KERBEROS DEMO, WROTE TABLE")

      printSuccess("Data written to Vertica")

      val dfRead: DataFrame = spark.read
        .format("com.vertica.spark.datasource.VerticaSource")
        .options(optionsKerberos + ("table" -> tableName))
        .load()

      dfRead.show()

      printSuccess("Data loaded back")

    } finally {
      spark.close()
    }
  }
}
