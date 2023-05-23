# Scala Examples

This project contains Scala code samples demonstrating the different capabilities and how to configure them. 

In general, you will need to include the Vertica-Spark connector to your scala project dependencies as shown in this
example's `build.sbt`. Then, it is a matter of correctly configuring the connector options.

Refer to our [readme](/README.md) for a full list of available options.

You can run the examples either on our docker environment or on your own Spark-Vertica cluster.

## Running Examples

First, ensure that the docker environment is up and running by following these [instructions](/examples/README.md). 

Start by assembling this example project into a fat jar with: 
```sh
sbt assembly
```
Make sure that you do this outside of the docker environment or else the process would be very slow.

Then, inside the client container, navigate to `/spark-connector/example/scala` and use `submit-examples.sh <example-name>`
to submit and run the specified example on our standalone cluster hosted on the docker environment. 
You can see the list of available examples by submitting it with no argument.

### Code Samples

The class `src/main/scala/example/Examples.scala` contains various functions that are our examples with the function names
correspond to the example name. For instance, `submit-examples.sh writeThenRead` would execute the function `writeThenRead()`.

Examples are expected to run without any exception thrown.

The file `src/main/resources/application.conf` contains common connector option values used by the examples. Feel free to 
override them to fit your setup.

### Using a modified Spark Connector

The example is using our latest Spark Connector release from Maven. Should you need to use a custom build of the connector (e.g. testing 
some changes), place the connector fat jar (from `sbt assembly`) inside this project's `lib`. Then, remove 
the connector dependency `"com.vertica.spark" % "vertica-spark" % s"${version.value}-slim"` from `build.sbt`.

The modified connector should now be applied when you build the example with `sbt assembly`

### S3 Examples

The example `writeThenReadWithS3` demonstrates how to configure the connector with an S3 file-store for use as the staging
area. As is, it is configured to run on our Minio containers acting as S3. 

To use your own S3 instance, edit the example's configurations with the appropriate settings. For more details on S3
configurations, check out our [readme](/README.md) and our [S3 instructions](/docs/s3-guide.md).

### GCS Examples

The example `writeThenReadWithGCS` demonstrates how to configure the connector to use a GCS bucket as the staging area.
**The example is not configured**, so make sure to edit the code sample with your GCS configurations. For more details on GCS
configurations, check out our [readme](/README.md) and our [GCS instructions](/docs/gcs-guide.md).

### Configuring `submit-examples.sh`

The `submit-examples.sh` script uses `spark-submit` underneath. Thus, you can configure it with any valid [configurations](https://spark.apache.org/docs/latest/submitting-applications.html). For example, to run on your own Vertica Spark cluster, change `--master` to your Spark driver URL.

### Running through SBT

Sometimes, you may find it more convenient to work inside SBT as you do not need to assemble a fat jar each time a change
was made. To do this, edit the Spark context inside `src/main/scala/example/Main.scala` to specify a local master
```
    val spark = SparkSession.builder()
      .master("local[*]")
      .appName("Vertica-Spark Connector Scala Example")
      .getOrCreate()
```

Then, to start the example use `sbt` to start the sbt server. To run the example, use `run <EXAMPLE_NAME>`

### Debugging

You can debug the examples through JVM remote debugging. If using sbt, start with sbt server with `sbt -jvm-debug localhost:5005`. If submitting to our standalone cluster, use `submit-examples-debug.sh` which will also open a remote debug port on `localhost:5005`. The process will wait for a connection before continuing.

Then, configure your IDE to connect to the remote debug address above.

# Kerberos Examples

The example `writeThenReadWithKerberos` demonstrates how to configure the connector with Kerberos.

To test using our kerberos docker environment, first make sure to shut down our docker environment if it is running. 
Navigate to `spark-connector/docker/` on your local machine and run
```sh
docker-compose down
```

Once finished, run the following to setup our docker environment and login to the client container:
```sh
docker-compose -f docker-compose-kerberos.yml up -d
docker exec -it client bash
```

Once finished, navigate to and use the script `submit-examples-kerberos.sh` to run the example.
