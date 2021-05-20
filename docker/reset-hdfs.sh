docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh

