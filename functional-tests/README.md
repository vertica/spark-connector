# Spark Connector - Functional Integration Tests

This project is in place to run a series of tests of the connector against a real Vertica database.

Configuration is specified with application.conf (HOCON format)

## How to run the tests

1. Start up HDFS and Vertica containers by running `docker-compose up -d` from this project's docker folder. 
2. Add this line to your local machine's /etc/hosts file: `127.0.0.1 hdfs`.
3. From the functional-tests directory, run `mkdir lib`. This is where the connector JAR will be copied to.
4. From the functional-tests directory, run `cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-1.0.jar ../functional-tests/lib && cd ../functional-tests`. This will build and copy the connector JAR file to the lib folder created in the previous step.
5. From the functional-tests directory, `sbt run` from the command line. Alternatively, to run the LargeDataTests, use `sbt "run Large"`.