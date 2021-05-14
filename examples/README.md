# How to run the examples

Make sure you have docker and sbt installed.
Tested using docker 20.10.0, sbt 1.4.1

Clone this repository:
```
git clone https://github.com/vertica/spark-connector.git
```
From the project's root directory:
```
cd docker
docker compose up -d
```
This will create a docker image for a client container and docker containers for a sandbox client environment and single-node clusters for both Vertica and HDFS.

Run the following commands to update the HDFS configuration files and restart HDFS:
```
docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
```

Run `docker exec -it docker_client_1 /bin/bash` to enter the sandbox client environment.

Now just run `sbt "run [CASE]"` from the `/spark-connector/examples/demo` directory.
