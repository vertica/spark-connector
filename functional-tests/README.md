# Spark Connector - Functional Integration Tests

This project is in place to run a series of tests of the connector against a real Vertica database.

Configuration is specified with application.conf (HOCON format)

## How to run the tests
From the functional-tests directory, run the following commands:
```
mkdir lib
cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-2.0.1.jar ../functional-tests/lib && cd ../functional-tests
```
This will create a lib folder and then build and copy the connector JAR file to it.

### Using HDFS:
From the project's docker directory:
```
cd ../docker
./sandbox-clientenv.sh
```
This will create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.

In the sandbox environment, change your working directory to functional-tests
```
cd spark-connector/functional-tests
```

From the functional-tests directory, `sbt run` from the command line. Alternatively, to run the LargeDataTests, use `sbt "run Large"`.

### Using S3:
Set the appropriate S3-credentials in the application.conf file. Refer to the following connector options on the project's [README](https://github.com/vertica/spark-connector#readme):
* aws_access_key_id
* aws_secret_access_key
* aws_region
* aws_session_token
* aws_credentials_provider
* aws_endpoint
* aws_enable_ssl

From the functional-tests directory, build the JAR file for the functional tests:
```
sbt assembly
```

From the project's docker directory:
```
cd ../docker
./sandbox-clientenv.sh
```
This will create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.

In the sandbox environment, change your working directory to functional-tests and run s3-functional-tests.sh
```
cd spark-connector/functional-tests
./s3-functional-tests.sh
```
