#!/usr/bin/env bash

service ssh start

# Override HDFS config
cp /hadoop/conf/*.xml /opt/hadoop/etc/hadoop

# Start HDFS services
rm -f /tmp/*.pid
start-dfs.sh
hadoop-daemon.sh start portmap
hadoop-daemon.sh start nfs3

# Copy test data to HDFS
while [ "$(hdfs dfsadmin -safemode get)" = "Safe mode is ON" ]; do sleep 1; done
hadoop fs -copyFromLocal /partitioned /3.1.1

echo "HDFS container is now running"

exec "$@"
