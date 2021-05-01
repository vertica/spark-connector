# Spark Connector - Performance Tests

This project is in place to run performance tests of the connector against a set of Spark, HDFS, and Vertica clusters.

Configuration is specified with application.conf (HOCON format)

## How to run the tests

1. Set up Vertica, HDFS and Spark. 
2. From the performance-tests directory, run `mkdir lib`. This is where the connector JAR will be copied to.
3. From the performance-tests directory, run `cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-1.0.jar ../performance-tests/lib && cd ../performance-tests`. This will build and copy the connector JAR file to the lib folder created in the previous step.
4. From the performance-tests directory, run sbt assembly to assemble the application jar
5. Use spark submit on the application jar, ie spark-submit --master spark://hdfs.example.com:7077 --deploy-mode cluster target/scala-2.12/spark-vertica-connector-performance-tests-assembly-1.0.jar          
 


## Tuning read performance

There are some connector parameters that may affect the performance of a read from Vertica operation.

num_partitions: Will set how many partitions are created, representing how many parallel executors will be reading data from the intermediate location at once. This should roughly correspond to the processing power / number of cores in the spark cluster.
max_file_size_export_mb and max_row_group_size_export_mb: Represent configuration of the files exported from Vertica to the intermediary location. These values default to where we find the best export performance lies. However, these can be tweaked dependending on details of the given clusters.

## Tuning write performance

On writing, the number of partitions is decided not by the connector, but by the number of partitions passed in. To change this, you can call the coalesce() function on a dataframe before writing it.

