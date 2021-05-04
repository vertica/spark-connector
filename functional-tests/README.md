# Spark Connector - Functional Integration Tests

This project is in place to run a series of tests of the connector against a real Vertica database.

Configuration is specified with application.conf (HOCON format)

## How to run the tests

From the project's root directory:
```
cd docker
docker build -t client .
docker-compose up -d
```
This will create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.

From the functional-tests directory, run the following commands:
```
mkdir lib
cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-1.0.jar ../functional-tests/lib && cd ../functional-tests
```
This will create a lib folder and then build and copy the connector JAR file to it.

From the functional-tests directory, `sbt run` from the command line. Alternatively, to run the LargeDataTests, use `sbt "run Large"`.