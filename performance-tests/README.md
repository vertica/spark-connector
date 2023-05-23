# Spark Connector - Performance Tests

This project is in place to run performance tests of the connector against a set of Spark, HDFS, and Vertica clusters.

Configuration is specified with `application.conf` (HOCON format).

## How to run the tests

1. Set up Vertica, HDFS and Spark
2. From the performance-tests directory, run `mkdir lib` to create the folder for the connector JAR
3. From the performance-tests directory, run `cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-<VERSION>.jar ../performance-tests/lib && cd ../performance-tests` to build and copy the connector JAR
4. From the performance-tests directory, run `sbt assembly` to assemble the test JAR
5. Use spark submit on the test JAR, such as `spark-submit --master spark://hdfs.example.com:7077 --deploy-mode cluster target/scala-2.12/spark-vertica-connector-performance-tests-assembly-<VERSION>.jar`

## Tuning read performance

The biggest factor in connector performance will be resources for Vertica and Spark. Vertica, particularly with default settings may run into a memory bottleneck. This can be improved via configuration of resource pools. 

### Vertica Resource Pool Configuration

The connector's Vertica-to-Spark functionality relies on a query to export data from Vertica to an intermediate filestore. This operation reserves a lot of memory, and the more memory available to it, the more threads it can create to parallelize the operation. 

It is suggested that the resource pool used for the operation is given as much memory as possible, and has its `plannedconcurrency` value set to as low as possible. 

For an explanation of this, any given Vertica query may only reserve its total provided memory divided by the `plannedconcurrency` value. A more detailed explanation can be found [here.](https://www.vertica.com/blog/do-you-need-to-put-your-query-on-a-budgetba-p236830/) The `plannedconcurrency` value sets how many independent queries are expected to be run, and the connector only uses one query at a time. This query is then parallelized by Vertica.

### Connector Options

There are some connector parameters that may affect the performance of a read from Vertica operation.

- `num_partitions`: Will set how many partitions are created, representing how many parallel executors will be reading data from the intermediate location at once. This should roughly correspond to the processing power / number of cores in the spark cluster.
- `max_file_size_export_mb` and `max_row_group_size_export_mb`: Represent configuration of the parquet files exported from Vertica to the intermediary location. These values default to where we find the best export performance lies: 16MB Row Group Size and 2048MB file size. However, these can be tweaked depending on details of the given clusters. 

## Tuning write performance

Similar steps to the above for tuning Vertica resource pools may be helpful for write performance.

On writing, the number of partitions is decided not by the connector, but by the number of partitions passed in. To change this, you can call the `coalesce()` function on a dataframe before writing it.
