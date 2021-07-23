library(sparklyr)

# Create a Spark config and disable Hive support to avoid errors
conf <- spark_config()
conf$sparklyr.connect.enablehivesupport <- FALSE

print("Connecting to Spark.")

# Connect to the Spark cluster
sc <- spark_connect(master = "spark://localhost:7077", version = "3.1", config = conf)

print("Connected to spark. Getting iris_tbl.")

# The Iris dataset comes with R and is used as test data
# Get the Iris data and store it in a Spark dataframe
iris_tbl <- sdf_copy_to(sc = sc, x = iris, overwrite = T)

print("Got iris_tbl. Writing source.")

# Write the Iris dataframe to the Vertica database
spark_write_source(iris_tbl, "com.vertica.spark.datasource.VerticaSource", "overwrite", list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Wrote source. Reading source.")

# Read the Iris data back from the Vertica database into a Spark dataframe
result <- spark_read_source(sc = sc, name = "example", source = "com.vertica.spark.datasource.VerticaSource", options = list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Finished reading source.")

# Print the dataframe's contents
print(result)

# Cleanup Spark connection
spark_disconnect(sc)

# Signal to run.r that it can terminate the Spark process
file.create("done")
