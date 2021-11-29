# How to run this example

Make sure you have Docker and SBT installed, and that Docker client is running. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository as mentioned in [examples](/examples/README.md).

## Build Connector Assembly
Change directory to the `connector` folder of the project:
```
cd /spark-connector/connector
```

Build the connector's assembly jar:
```
sbt assembly
```

## Add Connector Assembly as a Dependency to the Example Project
Create a `lib` folder under the `kerberos-example` folder and copy the assembled connector jar to it.
```
mkdir /spark-connector/examples/kerberos-example/lib
cp /spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-3.0.0.jar /spark-connector/examples/kerberos-example/lib
```

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
./sandbox-clientenv.sh kerberos
```


On Windows, you can run the equivalent batch file:

```
sandbox-clientenv.bat kerberos
```
This will put you in the sandbox client (i.e. client container) environment.

## Run test

The following steps assume you are in the client sandbox environment.

Change directory to the example project directory `spark-connector/examples/kerberos-example`, and run the example:

```
./run-kerberos-example.sh 
```

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


