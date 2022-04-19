# How to run this example

Make sure you have Docker and SBT installed, and that Docker client is running. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository as mentioned in [examples](/examples/README.md).

## (Optional) Using a modified Spark Connector
Assuming you have made changes to the spark connector 

Change directory to the `/connector` folder of the project:
```
cd /spark-connector/connector
```

Build the connector's assembly jar:
```
sbt assembly
```

Then create a `lib` folder at `/kerberos-example` and put the spark connector that you assembled inside.
```
mkdir /spark-connector/examples/kerberos-example/lib
cp /spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-3.2.0.jar /spark-connector/examples/kerberos-example/lib
```
Then in the example's `build.sbt`, comment out the vertica-spark connector dependency.

Currently, the example `build.sbt` will default to pulling the latest published spark connector and ignore
the `/lib` folder. Commenting out the dependency and sbt will use the `/lib` folder instead.


## Build Example Assembly

Change directory to the kerberos example project `examples/kerberos-example`.

Build the assembly if it hasn't been built already. Note: avoid building it within docker, it may be very slow.

```
sbt assembly
```

## Prepare test environment

If you have used the `sandbox-clientenv.sh` script to create a docker environment already, please do a `docker compose down` before continuing.

In the docker folder (`spark-connector/docker`), run this command to start the vertica, spark, hdfs, kerberos services and enter the CLI environment:

```
./sandbox-clientenv.sh -k
```


On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat -k
```
This will put you in the sandbox client (i.e. client container) environment. 

By default, the script will pull the [latest](https://hub.docker.com/r/vertica/vertica-k8s) Vertica docker image. To use an older version of Vertica, you can specify a specific tag by appending the option `-v [TAG]`. For example, to use Vertica 10.1.1-0:

```
./sandbox-clientenv.sh -k -v 10.1.1-0
```

```
./sandbox-clientenv.bat -k -v 10.1.1-0
```

## Run test

The following steps assume you are in the client sandbox environment.

Change directory to the example project directory `spark-connector/examples/kerberos-example`, and run the example:

```
./run-kerberos-example.sh 
```

## Debugging.

To debug, run `./run-kerberos-example.sh debug`. The execution will wait on port `5005` until a debugger connects to it before continuing with the execution. 

To change the port number, edit `docker-compose-kerberos.yml`, currently, `docker_krbclient_1` container is mapping its `5005` port to host's `5005`.

## Tear down containers

Exit out of the interactive terminal by running `exit`. 

From the docker folder, run the following:

```
docker compose -f docker-compose-kerberos.yml down
```

## Rebuilding the images

If, for some reason, you have made changes to the Dockerfiles used in the Kerberos setup, you can rebuild them by running the following:

```
docker compose -f docker-compose-kerberos.yml up --build
```


