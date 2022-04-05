@echo off
@REM setlocal EnableDelayedExpansion

set VERTICA_VERSION=%~2
IF "%~1" == "kerberos" (
	call sandbox-kerberos-clientenv.bat
) ELSE (
	echo "running non-kerberized docker compose"
	echo "with vertica version: %VERTICA_VERSION%"
    docker compose -f docker-compose-test.yml up -d
    docker exec docker_vertica_1 /bin/sh -c "opt/vertica/bin/admintools -t create_db --database=docker --password='' --hosts=localhost"
    docker exec docker_vertica_1 /bin/sh -c "sudo /usr/sbin/sshd -D"
	docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
	docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
	docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
	docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
	docker cp ../functional-tests/src/main/resources/3.1.1 docker_hdfs_1:/partitioned
    docker exec docker_hdfs_1 hadoop fs -copyFromLocal /partitioned /3.1.1
	docker exec docker_vertica_1 vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"
	docker exec -it docker_client_1 /bin/bash
)
