#How to run the examples

Clone this repository:
```
git clone https://github.com/vertica/spark-connector.git
```

From the project's root directory:
```
cd docker
docker-compose up -d
```
This will create docker containers for single-node clusters for both Vertica and HDFS.

Next, change directory to the example you want to. For example, if you want to run the "demo" example:
```
cd examples/demo
```
Now create a lib directory:
```
mkdir lib
```
Put the assembly jar for the connector in the lib folder you just created. If you don't have the jar file, you can download it here: https://github.com/vertica/spark-connector/releases 

Before you run any of the examples, you will need to add the following entry to your /etc/hosts file:
```
127.0.0.1 hdfs
```

Now just run `sbt run` from the `examples/demo` directory.