# How to run the examples

Make sure you have Docker and sbt installed, and that Docker client is running. Tested using docker 20.10.0, sbt 1.4.1

Clone this repository:

```
git clone https://github.com/vertica/spark-connector.git
```

If running examples on Windows, make sure that all scripts in the repository are correctly encoded:

- `.bat` scripts as `Windows (CR LF)`
- `.sh` scripts as `Unix (LF)`.

Prior to running the same test again, or switching to another test example, make sure to perform cleanup and containers tear down procedure after each run.

**Note**: _The instructions for running the S3, Sparklyr, Kerberos and PySpark examples are different. If you would like to run those examples, please see their respective READMEs._

## Prepare test environment

From the docker folder in the project's root directory (`spark-connector/docker`), run:

```
./sandbox-clientenv.sh
```

On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat
```

This will:

1. Create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.
2. Update the HDFS configuration files and restart HDFS
3. Enter the sandbox client environment (i.e. command prompt of `docker_client` container).

This will put you in the sandbox client (i.e. client container) environment.

By default, the script will pull the [latest](https://hub.docker.com/r/vertica/vertica-k8s) Vertica docker image. To use an older version of Vertica, you can specify a specific tag by appending the option `-v [TAG]`. For example, to use Vertica 10.1.1-0:

```
./sandbox-clientenv.sh -v 10.1.1-0
```

```
./sandbox-clientenv.bat -v 10.1.1-0
```

## Run test

The following steps assume you are in the client sandbox environment. 

Proceed running tests using one of the following methods:

### Using 'sbt run'

Change your working directory to one in `spark-connector/examples/[EXAMPLE]`, such as `spark-connector/examples/basic-read`, then enter:
```
sbt run
```
**Note**: Some examples may throw exceptions on exit. To check that the example executed correctly, the console output should contain a SUCCESS message.

If you decide to run the demo example from the `/spark-connector/examples/demo` directory, run:
```
sbt "run [CASE]"
```

Cases to choose from:

- columnPushdown
- filterPushdown
- writeAppendMode
- writeOverwriteMode
- writeErrorIfExistsMode
- writeIgnoreMode
- writeCustomStatement
- writeCustomCopyList

### Running Application in Spark Cluster

**Note**: _This method is required for the S3-example._

In the example's root directory (`spark-connector/examples/[EXAMPLE]`) run:

```
sbt assembly
```

From the docker directory in `spark-connector/docker`, run:

```
./sandbox-clientenv.sh
```

On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat
```

The following steps assume you are in the client sandbox environment.

Change your working directory to `spark-connector/examples`, then run:

```
./run-example.sh [REPLACE WITH EXAMPLE DIR]
```

Example argument: basic-read-example

### If using Thin Jar

If you are using the thin jar and running into an error similar to the following:
`java.lang.NoSuchMethodError: 'void cats.kernel.CommutativeSemigroup.$init$(cats.kernel.CommutativeSemigroup)'`, you may need to shade the cats dependency in your project.

This can be done by adding the following to your build.sbt file:

```
assembly / assemblyShadeRules := {
    val shadePackage = "com.azavea.shaded.demo"
    Seq(
        ShadeRule.rename("cats.kernel.**" -> s"$shadePackage.cats.kernel.@1").inAll
    )
} 
```

## Output file

To only see the example output without sbt logs, you can write the result to a file by appending the following to any run command: 
```
[run command] > output.txt
```

You may then view this file using the command `cat output.txt`.

## Tear down containers

Once you're finished running the example, exit out of the interactive terminal by running `exit`. 

Make sure you are still in the docker directory, then run: 
```
docker-compose down
```
This will shut down and remove the containers safely.

## Note

There are additional prerequisites to run the S3, Pyspark, Sparklyr, or Kerberos examples. If you want to run these, please take a look at their respective README files.
