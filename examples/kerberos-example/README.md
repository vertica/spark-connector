# How to run this example

Clone the connector repository and in the docker folder, run this command:
```
./sandbox-clientenv.sh kerberos
```

You will now be in the sandbox container. Run kinit to get a TGT:
```
kinit user1
```

The password for this user is "user1".

Change directory to the example project:
```
cd /spark-connector/examples/kerberos-example
```

Run the example:
```
./run-example.sh kerberos-example/target/scala-2.12/spark-vertica-connector-kerberos-example-assembly-2.0.1.jar
``` 

# Tearing down the containers

From the docker folder, run the following:
```
docker compose -f docker-compose-kerberos.yml down
```