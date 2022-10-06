# Jupyter Notebook Examples

## Creating the Jupyter Notebook Docker Container

In order to run these examples the Jupyter container must be created and started.  See the [Docker README](/docker/README.md) on how to do this by using the correct Docker profile.

## General Notebook Configuration

In order for Jupyter to communicate with Spark, Hadoop, Vertica, etc it must be on the same network.  Our Docker environment does this for you. 

The Spark Connector JAR must also be available in order to load the JAR and send it to Spark.  The entire Spark Connector repo is mounted in the Docker container, including the directory containing the Spark Connector JAR (if you build it yourself).  Otherwise you must download the JAR from [Maven](https://mvnrepository.com/artifact/com.vertica.spark/vertica-spark).

To start a new Spark session must be created, pointing to the Spark master as well as loading the Spark Connector JAR.  For example:
```py
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.master", "spark://spark:7077")
    .config("spark.driver.memory", "2G")
    .config("spark.executor.memory", "1G")
    .config("spark.jars", "/spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-3.3.3.jar")
    .getOrCreate())
sc = spark.sparkContext
```

Once that is complete the Spark context may be used to read and write data using the Vertica Spark Connector data source ("com.vertica.spark.datasource.VerticaSource").

Note that Jupyter Notebook previously bundled the Spylon kernel so that Scala could be used, but that kernel has not been maintained and is no longer included in Jupyter Notebook by default.

## Running a Notebook

1. Go to http://localhost:8888/ and login with the token "test"
2. Under the File Browser on the left, navigate to the work folder and open the desired example Jupyter Notebook
3. Execute the cells, in order, using the Run button
