install.packages("curl", repo = "http://cran.us.r-project.org")
install.packages("sparklyr", repo = "http://cran.us.r-project.org")
library(sparklyr)

# Create a Spark config and disable Hive support to avoid errors
config <- spark_config()
config$sparklyr.jars.default <- "../../connector/target/scala-2.12/spark-vertica-connector-assembly-3.3.1.jar"
config$sparklyr.connect.enablehivesupport <- FALSE
config$sparklyr.appName <- "Vertica Spark Connector Sparklyr example"

print("Connecting to Spark.")

# Connect to the Spark cluster
sc <- spark_connect(master="spark://spark:7077", version = "3.1", config = config)

print("Connected to spark. Getting iris_tbl.")

# The Iris dataset comes with R and is used as test data
# Get the Iris data and store it in a Spark dataframe
iris_tbl <- sdf_copy_to(sc = sc, x = iris, overwrite = T)

print("Got iris_tbl. Writing to Vertica.")

# Write the Iris dataframe to the Vertica database
spark_write_source(iris_tbl, "com.vertica.spark.datasource.VerticaSource", "overwrite", list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Wrote to Vertica. Reading from Vertica.")

# Read the Iris data back from the Vertica database into a Spark dataframe
result <- spark_read_source(sc = sc, name = "example", source = "com.vertica.spark.datasource.VerticaSource", options = list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("Finished reading.")

# Print the dataframe's contents
print(result)

# Cleanup Spark connection
spark_disconnect(sc)
