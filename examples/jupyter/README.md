# Jupyter Notebook Examples

## Creating the Jupyter Notebook Docker Container

In order to run these examples the Jupyter container must be created and started.  In order to do this, you will first need to navigate to the ```spark-connector/docker``` directory. From there you can start the Docker containers with the "jupyter" profile:
```sh
docker-compose --profile jupyter up -d
```

An important thing to note is that the Spark and Python versions for Spark (master and worker nodes) and Jupyter Notebook must match, otherwise it will not work.  Our Docker environment ensures the Python and Spark versions between these images are in-sync.

For more information see the [Docker README](/docker/README.md).

## Running a Notebook

1. Go to http://localhost:8888/ and login with the token "test"
2. Under the File Browser on the left, navigate to the work folder and open the desired example Jupyter Notebook
3. Execute the cells, in order, using the Run button or by pressing ```Shift-Enter``` 

## Examples

### Basic Read & Write

A simple read and write that uses a two column schema of a string and an integer.

### Complex Array

A Spark job that writes a regular array, nested array, and an array representative of a hash map.

### Linear Regression

A Machine Learning example that utilizes Spark's Linear Regression algorithm. This job also contains reading and importing a .csv into Vertica.


Each Notebook Example is annotated and written in a way to walk the user step-by-step through a Spark job to Vertica.

## ARM Limitations

Due to limited compatability with Docker, if you are running these examples on an ARM-based machine do note that there may be performance issues or failure of connection between containers.

## General Notebook Configuration

Jupyter must be able to communicate with Spark, Hadoop, Vertica, etc, so it must be on the same Docker network.  Our Docker environment configures this for you. 

The Spark Connector JAR must also be available in order to load the JAR and send it to Spark.  The entire Spark Connector repo is mounted in the Docker container, including the directory containing the Spark Connector JAR (if you build it yourself).  Otherwise you must download the JAR from [Maven](https://mvnrepository.com/artifact/com.vertica.spark/vertica-spark) and reference the location in your environment.

A new Spark session must be created, pointing to the Spark master as well as loading the Spark Connector JAR.  For example:
```py
from pyspark.sql import SparkSession

spark = (SparkSession.builder
    .config("spark.master", "spark://spark:7077")
    .config("spark.driver.memory", "2G")
    .config("spark.executor.memory", "1G")
    .config("spark.jars", "/spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-<VERSION>.jar")
    .getOrCreate())
sc = spark.sparkContext
```

Once that is complete the Spark context may be used to read and write data using the Vertica Spark Connector data source ("com.vertica.spark.datasource.VerticaSource").  See the example Jupyter Notebooks in this folder.

Note that Jupyter Notebook previously bundled the Spylon kernel so that Scala could be used, but that kernel has not been maintained and is no longer included in Jupyter Notebook by default.  As a result it is recommended to use the Python kernel in Jupyter Notebook.
