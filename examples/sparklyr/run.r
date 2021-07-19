install.packages("sparklyr", repo = "http://cran.us.r-project.org")
# install.packages("devtools", repo = "http://cran.us.r-project.org")
# devtools::install_github("sparklyr/sparklyr")
library(sparklyr)

#spark_install(version = "3.0")

config <- spark_config()
config[["spark.driver.extraClassPath"]] <- "../../connector/target/scala-2.12/spark-vertica-connector-assembly-2.0.0.jar"
config[["sparklyr.log.console"]] <- TRUE

Sys.setenv(SPARK_HOME = "/opt/spark", JAVA_HOME = "/usr/lib/jvm/jre-11-openjdk")

spark_submit(master = "local", file = "sparkapp.r", config = config)