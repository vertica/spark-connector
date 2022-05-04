install.packages("sparklyr", repo = "http://cran.us.r-project.org")
library(sparklyr)

# Delete the "done" file if it already exists, since we need to wait for it to be created later
if (file.exists("done")) unlink("done")

# Create the Spark config and give access to our connector jar file
config <- spark_config()
config$sparklyr.jars.default <- "../../connector/target/scala-2.12/spark-vertica-connector-assembly-3.2.1.jar"

# Submit a new Spark job that executes sparkapp.r with Spark version 3.1
spark_submit(master = "spark://localhost:7077", version = "3.1", file = "sparkapp.r", config = config)

# Wait until the Spark job creates the file "done". Otherwise, this program will terminate and kill the Spark process too early.
retries <- 120
while (!file.exists("done") && retries > 0) {
  Sys.sleep(1)
  retries <- retries - 1
}
