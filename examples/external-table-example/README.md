#External Tables

##New Data
To create an external table by writing new data from Spark to HDFS, you need to specify the "create_external_table" connector option as "new-data" or "true". This option forces the connector to bypass the copy from HDFS to Vertica and instead creates an external table out of the newly written data. You will also need to specify the location of the data using the "staging_fs_url" connector option ("webhdfs://hdfs:50070/data/data.parquet/").

####Example:

```scala
val opts = Map(
    "host" -> "vertica_hostname",
    "user" -> "vertica_user",
    "db" -> "db_name",
    "password" -> "db_password",
    "staging_fs_url" -> "hdfs://hdfs-url:7077/data",
    "table" -> "tablename",
    "create_external_table" -> "new-data"
  )

// Create data and put it in a dataframe
val schema = new StructType(Array(StructField("col1", IntegerType)))
val data = Seq(Row(77))
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

val mode = SaveMode.ErrorIfExists

// Write the dataframe to HDFS and create external table
df.write.format("com.vertica.spark.datasource.VerticaSource").options(opts).mode(mode).save()
```

##Existing Data
The example project in this folder creates an external table from data existing on the disk. In order to accomplish this, you need to specify the "create_external_table" option as "existing-data". Also, ensure that the spark dataframe you are writing is empty. specify the location of the data using the "staging_fs_url" connector option ("webhdfs://hdfs:50070/data/data.parquet/").

__Note__: 
Partitioned columns  will require you provide a partial schema detailing those columns.

####Example providing a partial schema for partitioned data:

```scala
val writeOpts = Map(
  "host" -> "vertica_hostname",
  "user" -> "vertica_user",
  "db" -> "db_name",
  "password" -> "db_password"
  )

val tableName = "existingData"
// Path to existing data that contains partitioned data
val filePath = "webhdfs://hdfs:50070/3.1.1/"
// Partial schema passed to the connector that provides data type for partitioned column
val schema = new StructType(Array(StructField("col1", IntegerType)))
// Empty dataframe with partial schema written using connector
val df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
// SaveMode is ignored if set to Overwrite
val mode = SaveMode.Overwrite
// Add in relevant connector options with write
df2.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("staging_fs_url" -> filePath, "table" -> tableName, "create_external_table" -> "existing-data")).mode(mode).save()
```

####Example overwriting col size using individual column metadata:

__Note__: Using the method to override the column sizes in the following example is __OPTIONAL__. If you opt to not override the size of varchar and varbinary columns, the connector will default the size to 1024 and 65000 bytes for each varchar or varbinary column, respectively.

```scala
val writeOpts = Map(
  "host" -> "vertica_hostname",
  "user" -> "vertica_user",
  "db" -> "db_name",
  "password" -> "db_password"
  )

val tableName = "existingData"
// Path to existing data that contains existing data
val filePath = "webhdfs://hdfs:50070/existingData"
// Partial schema passed to the connector that provides columns that we want to overwrite with new size
val schema = new StructType(Array(StructField("col1", BinaryType), StructField("col2", StringType)))
// Map containing column names and new sizes
val columnLengthMap = Map(
  "col1" -> 256,
  "col2" -> 256
)
// Empty dataframe with partial schema written using connector
var df2 = spark.createDataFrame(spark.sparkContext.emptyRDD[Row], schema)
//Modified dataframe with added metadata
columnLengthMap.foreach { case (colName, length) =>
  val metadata = new MetadataBuilder().putLong("maxlength", length).build()
  df2 = df2.withColumn(colName, df2(colName).as(colName, metadata))
}
// SaveMode is ignored if set to Overwrite
val mode = SaveMode.Overwrite
// Add in relevant connector options with write
df2.write.format("com.vertica.spark.datasource.VerticaSource").options(writeOpts + ("staging_fs_url" -> filePath, "table" -> tableName, "create_external_table" -> "existing-data")).mode(mode).save()
```
