# How to run this example

Make sure you have Docker and SBT installed. Tested using docker 3.3.1, sbt 1.4.1

Clone the following repository to get the Kerberized Docker setup:
```
git clone https://github.com/jonathanl-bq/vertica-testenv
```

Change directory to the connector directory of this environment and clone the connector repository:
```
cd vertica-testenv/spark-connector
git clone https://github.com/vertica/spark-connector.git
```

Change directory back to the root of the vertica-testenv project and start up the containers:
```
cd ..
./vertica_testenv start
./vertica_testenv sandbox
```

You will now be in the sandbox container. Run kinit to get a TGT:
```
kinit user1
```

The password for this user is "user1".

Change directory to the example project:
```
cd connector/spark-connector/examples/kerberos-example
```

Run `sbt assembly` to build the assembly jar for this example (you may want to do this outside the container for a faster build).

Run the example:
`./run-example.sh target/scala-2.12/spark-vertica-connector-kerberos-example-assembly-1.0.jar`

If you get a permission denied issue, run `chmod +x run-example.sh`