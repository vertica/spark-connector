# Spark Connector - Functional Integration Tests

This project is in place to run a series of tests of the connector against a real Vertica database.

Configuration is specified with application.conf (HOCON format)

## How to run the tests
From the functional-tests directory, run the following commands:
```
mkdir lib
cd ../connector && sbt assembly && cp target/scala-2.12/spark-vertica-connector-assembly-3.2.1.jar ../functional-tests/lib && cd ../functional-tests
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

### Starting Functional Tests

To execute the default functional tests, start the `sbt` server from the command line. Enter `run` to execute the default test suites. You can specify arguments to modify you test run. For more details, use `run -h`.

As an example, to include large data tests into the run, use `run -l`. Using `run -s ComplexTypeTests` will only execute ComplexTypeTests.

To run a specific test in a suite, use option `-t` to specify a test name. For example `run -s ComplexTypeTests -t "<test-name-here>"` will execute the specified tests in the suite. Note that option `-t` has to be used with option `-s`.

### Using S3:
Set the appropriate S3-credentials in the application.conf file. Refer to the following connector options on the project's [README](https://github.com/vertica/spark-connector#readme):
* aws_access_key_id
* aws_secret_access_key
* aws_region
* aws_session_token
* aws_credentials_provider
* aws_endpoint
* aws_enable_ssl
* aws_enable_path_style

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

### Using GCS:
Currently, we do not have a solution for emulating GCS. Thus, you will need to obtain access to your own GCS bucket for testing.
Follow the [GCS manual](../GCSUserManual.md) to obtain the needed credentials. Then, add the following connector options to the project's configuration file:
```
gcs_hmac_key_id
gcs_hmac_key_secret
gcs_service_key_id
gcs_service_key
gcs_service_email
```
Make sure your update the option `filepath` to your gcs bucket as well. Then start the test with `sbt run`.
