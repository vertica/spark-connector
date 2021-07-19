library(sparklyr)
library(dplyr)

# spark_install(version = "3.1")

print("connecting to spark")

Sys.setenv(SPARK_HOME = "/opt/spark", JAVA_HOME = "/usr/lib/jvm/jre-11-openjdk")

conf <- spark_config()
conf$`sparklyr.cores.local` <- 4
conf$`sparklyr.shell.driver-memory` <- "16G"
conf$spark.memory.fraction <- 0.9
conf$sparklyr.log.console <- TRUE

sc <- spark_connect(master = "local", config = conf)

print("connected to spark. Getting iris_tbl")

iris_tbl <- sdf_copy_to(sc = sc, x = iris, overwrite = T)

print("got iris_tbl. writing source")

spark_write_source(iris_tbl, "com.vertica.spark.datasource.VerticaSource", "overwrite", list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("wrote source. reading source")

spark_read_source(sc = sc, source = "com.vertica.spark.datasource.VerticaSource", options = list(
  "host" = "vertica",
  "user" = "dbadmin",
  "password" = "",
  "db" = "docker",
  "staging_fs_url" = "webhdfs://hdfs:50070/data/dirtest",
  "table" = "iris"
))

print("finished reading source")

spark_disconnect(sc)