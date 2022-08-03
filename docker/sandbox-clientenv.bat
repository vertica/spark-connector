@echo off

set WORKERS_COUNT=1
:GETOPTS
if /I "%1" == "-v" set VERTICA_VERSION=%2& shift
if /I "%1" == "-s" set SPARK_INSTALL=%2& shift
if /I "%1" == "-k" set KERBEROS=1 & shift
if /I "%1" == "-w" set WORKERS_COUNT=%2& shift
shift
if not "%1" == "" goto GETOPTS

if defined KERBEROS (
	call sandbox-kerberos-clientenv.bat
) ELSE (
	echo "running non-kerberized docker compose"
    docker compose -f docker-compose.yml up -d --scale spark-worker=%WORKERS_COUNT%
    docker exec docker_vertica_1 /bin/sh -c "opt/vertica/bin/admintools -t create_db --database=docker --password='' --hosts=localhost"
    docker exec docker_vertica_1 /bin/sh -c "sudo /usr/sbin/sshd -D"
	docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
	docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
	docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
	docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
	docker cp ../functional-tests/src/main/resources/3.1.1 docker_hdfs_1:/partitioned
    docker exec docker_hdfs_1 hadoop fs -copyFromLocal /partitioned /3.1.1
	docker exec docker_vertica_1 vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"
	docker exec docker_vertica_1 vsql -c "select version();"
	docker exec -it docker_client_1 /bin/bash
)
