library(sparklyr)

conf <- spark_config()
conf$sparklyr.connect.enablehivesupport <- FALSE

print("Connecting to Spark.")

sc <- spark_connect(master = "spark://localhost:7077", version = "3.1", config = conf)

print("Connected to spark. Getting iris_tbl.")

iris_tbl <- sdf_copy_to(sc = sc, x = iris, overwrite = T)

print("Got iris_tbl. Writing source.")

spark_write_source(iris_tbl, "com.vertica.spark.datasource.VerticaSource", "overwrite", list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Wrote source. Reading source.")

result <- spark_read_source(sc = sc, name = "example", source = "com.vertica.spark.datasource.VerticaSource", options = list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Finished reading source.")

# Signal to run.r that it can terminate the Spark process
spark_write_csv(result, "batch.csv")

print("Finished writing batch.csv.")

# Cleanup Spark connection
spark_disconnect(sc)