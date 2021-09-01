# How to run this example

Make sure you have Docker and SBT installed. Tested using docker 3.3.1 and SBT 1.4.1.

First, clone the connector repository.

In this (kerberos-example) folder, run `sbt assembly` to build the project jar.

In the docker folder, run this command:
```
./sandbox-clientenv.sh kerberos
```

Change directory to the example project:
```
cd /spark-connector/examples/kerberos-example
```

Run the example:
```
./run-example.sh ./target/scala-2.12/spark-vertica-connector-kerberos-example-assembly-2.0.1.jar
``` 

# Tearing down the containers

From the docker folder, run the following:
```
docker compose -f docker-compose-kerberos.yml down
```