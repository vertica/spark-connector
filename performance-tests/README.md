# Spark Connector - Performance Tests

This project is in place to run performance tests of the connector against a set of Spark, HDFS, and Vertica clusters.

Configuration is specified with application.conf (HOCON format)

## How to run the tests

1. Set up Vertica, HDFS and Spark. 
2. From the performance-tests directory, run `mkdir lib`. This is where the connector JAR will be copied to.
3. From the performance-tests directory, run `cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-1.0.jar ../performance-tests/lib && cd ../performance-tests`. This will build and copy the connector JAR file to the lib folder created in the previous step.
4. From the performance-tests directory, run sbt assembly to assemble the application jar
5. Use spark submit on the application jar, ie spark-submit --master spark://hdfs.example.com:7077 --deploy-mode cluster target/scala-2.12/spark-vertica-connector-performance-tests-assembly-1.0.jar          
 
