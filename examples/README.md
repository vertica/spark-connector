# How to run the examples

**Note**: The instructions for running the S3, Sparklyr, Kerberos and PySpark examples are different. If you would like to run those examples, please see their respective READMEs.

Make sure you have docker and sbt installed.
Tested using docker 20.10.0, sbt 1.4.1

Clone this repository:
```
git clone https://github.com/vertica/spark-connector.git
```

## Using 'sbt run'
From the project's root directory:
```
cd docker
./sandbox-clientenv.sh
```
On Windows, you can run the equivalent batch file:
```
cd docker
sandbox-clientenv.bat
```
This will:
1. Create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.


2. Update the HDFS configuration files and restart HDFS


3. Enter the sandbox client environment.

Now change your working directory to one in `spark-connector/examples` 

After changing your directory to a specific example (such as spark-connector/examples/basic-read), just enter `sbt run`

If you decide to run the demo example from the `/spark-connector/examples/demo` directory, run `sbt "run [CASE]"` to run the various cases.

Cases to choose from:
- columnPushdown
- filterPushdown
- writeAppendMode
- writeOverwriteMode
- writeErrorIfExistsMode
- writeIgnoreMode
- writeCustomStatement
- writeCustomCopyList

To only see the example output without sbt logs, you can write the result to a file using `sbt "run [CASE]" > output.txt `. You may then view this file using the command `cat output.txt`.

Once you're finished running the examples exit out of the interactive terminal by running `exit`. When you are done using the containers, make sure you are in the docker directory and run: `docker-compose down`. This will shut down and remove the containers safely.

## Running Example Application in Spark Cluster

Note: this method is required for the S3-example. 

In the example's root directory (spark-connector/examples/[EXAMPLE]) , run `sbt assembly`:
```
sbt assembly
```

From the example's root directory:
```
cd ../../docker
./sandbox-clientenv.sh
```
On Windows, you can run the equivalent batch file:
```
cd docker
sandbox-clientenv.bat
```

Now change your working directory to the examples directory `spark-connector/examples` 

Once in the examples directory, run:
```
./run-example.sh [REPLACE WITH EXAMPLE DIR]/target/scala-2.12/spark-vertica-connector-[REPLACE WITH EXAMPLE DIR]-assembly-2.0.1.jar
``` 
Example argument: basic-read-example/target/scala-2.12/spark-vertica-connector-basic-read-example-assembly-2.0.1.jar

### Using Thin Jar
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

### Note

There are additional prerequisites to run the S3, Pyspark, Sparklyr, or Kerberos examples. If you want to run these, please take a look at their respective README files.
