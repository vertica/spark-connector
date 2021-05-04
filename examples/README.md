#How to run the examples

Make sure you have docker, docker-compose, and sbt installed.

Clone this repository:
```
git clone https://github.com/vertica/spark-connector.git
```

From the project's root directory:
```
cd docker
docker-compose up -d
```
This will create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.

Next, change directory to the example you want to run. For example, if you want to run the "demo" example:
```
cd examples/demo
```
Now create a lib directory:
```
mkdir lib
```
Put the assembly jar for the connector in the lib folder you just created. If you don't have the jar file, you can download it here: https://github.com/vertica/spark-connector/releases

Run the following commands to update the HDFS configuration files and restart HDFS:
```
docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
```

Run `docker exec -it docker_client_1 /bin/bash` to enter the sandbox client environment.

Now just run `sbt run` from the `/spark-connector/examples/demo` directory.