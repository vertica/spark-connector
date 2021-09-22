# How to run this example

Make sure you have Docker and SBT installed. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository as mentioned in [examples](/examples/README.md).

Change directory to the `connector` folder of the project:
```
cd /spark-connector/connector
```

Build the connector's assembly jar:
```
sbt assembly
```

Create a `lib` folder under the `kerberos-example` folder and copy the assembled connector jar to it.
```
mkdir /spark-connector/examples/kerberos-example/lib
cp /spark-connector/connector/target/scala-2.12/spark-vertica-connector-assembly-2.0.2.jar /spark-connector/examples/kerberos-example/lib
```

In the docker folder, run this command to start the vertica, spark, hdfs, kerberos services and enter the CLI environment:
```
./sandbox-clientenv.sh kerberos
```

Change directory to the example project:
```
cd /spark-connector/examples/kerberos-example
```

Build the assembly jar for the `kerberos-example` if it hasn't been built already.  Note: building within docker may be slower.
```
sbt assembly
```

Run the example:
```
./run-kerberos-example.sh 
``` 

# Rebuilding the images

If, for some reason, you have made changes to the Dockerfiles used in the Kerberos setup, you can rebuild them by running the following:
```
docker compose -f docker-compose-kerberos.yml up --build
```

# Tearing down the containers

From the docker folder, run the following:
```
docker compose -f docker-compose-kerberos.yml down
```
