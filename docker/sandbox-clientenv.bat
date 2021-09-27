@echo off

IF "%~1" == "kerberos" (
	call sandbox-kerberos-clientenv.bat
) ELSE (
	echo "running non-kerberized docker compose"
	docker compose -f docker-compose.yml up -d
	docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
	docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
	docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
	docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
	docker cp ../functional-tests/src/main/resources/3.1.1 docker_hdfs_1:/partitioned
    docker exec docker_hdfs_1 hadoop fs -copyFromLocal /partitioned /3.1.1
	docker exec docker_vertica_1 vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"
	docker exec -it docker_client_1 /bin/bash
)
