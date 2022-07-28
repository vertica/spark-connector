function configure_kdc() {
  docker exec kdc /kdc/configure.sh
}

function configure_db() {
  docker exec vertica /bin/sh -c "opt/vertica/bin/admintools -t create_db --database=docker --password='' --hosts=localhost"
  docker exec vertica /bin/sh -c "sudo /usr/sbin/sshd -D"
  docker exec -u 0 vertica /vertica-krb/kerberize.sh
}

function configure_hdfs() {
  docker exec hdfs service ssh start
  docker exec hdfs start-dfs.sh
  docker exec -u 0 hdfs /hdfs-krb/kerberize.sh
  docker exec hdfs stop-dfs.sh
  docker exec hdfs start-dfs.sh
}

function configure_client() {
  docker exec docker_krbclient_1 /client-krb/kerberize.sh
  docker exec vertica /bin/sh -c "echo $(docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" hdfs) hdfs.example.com hdfs | sudo tee -a /etc/hosts"
  docker exec docker_krbclient_1 /bin/sh -c "echo $(docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" hdfs) hdfs.example.com hdfs | tee -a /etc/hosts"
}

function configure_containers() {
  echo "configuring kdc"
  configure_kdc
  echo "configuring hdfs"
  configure_hdfs
  echo "configuring client"
  configure_client
  echo "configuring db"
  configure_db
}

WORKERS_COUNT=1
while getopts 'kvsw:' c
do
  case $c in
    k) KERBEROS=1 ;;
    v) export VERTICA_VERSION=$OPTARG ;;
    s) export SPARK_INSTALL=$OPTARG ;;
    w) export WORKERS_COUNT=$OPTARG ;;
  esac
done

if [ "$KERBEROS" == "1" ]
  then
    echo "running kerberos docker compose"
    docker compose -f docker-compose-kerberos.yml up -d --scale docker-worker=$WORKERS_COUNT
    configure_containers
    docker exec -it docker_krbclient_1 /bin/bash
else
  echo "running non-kerberized docker compose"
  docker compose -f docker-compose.yml up -d
  docker exec docker_vertica_1 /bin/sh -c "opt/vertica/bin/admintools -t create_db --database=docker --password='' --hosts=localhost"
  docker exec docker_vertica_1 /bin/sh -c "sudo /usr/sbin/sshd -D"
  docker exec docker_client_1 /bin/sh -c "cp /etc/hadoop/conf/* /hadoop-3.3.0/etc/hadoop/"
  docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
  docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
  docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
  docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
  docker cp ../functional-tests/src/main/resources/3.1.1 docker_hdfs_1:/partitioned
  docker exec docker_hdfs_1 hadoop fs -copyFromLocal /partitioned /3.1.1
  docker exec docker_vertica_1 vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"
  docker exec -w /spark-connector/docker/cluster spark-driver /bin/bash -c './start-master.sh'
  docker exec -w /spark-connector/docker/cluster spark-worker-1 /bin/bash -c './start-worker.sh'
  docker exec -w /spark-connector/docker/cluster spark-worker-2 /bin/bash -c './start-worker.sh'
  docker exec -it docker_client_1 /bin/bash
fi
