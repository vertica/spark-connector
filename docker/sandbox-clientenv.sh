function configure_kdc() {
  docker exec docker_kdc_1 /kdc/configure.sh
}

function configure_db() {
  sleep 10
  docker exec -u 0 docker_vertica_1 /vertica-krb/kerberize.sh
}

function configure_hdfs() {
  docker exec -u 0 docker_hdfs_1 /hdfs-krb/kerberize.sh
  docker exec docker_hdfs_1 stop-dfs.sh
  docker exec docker_hdfs_1 start-dfs.sh
}

function configure_client() {
  docker exec docker_client_1 /client-krb/kerberize.sh
}

function configure_containers() {
  echo "configuring kdc"
  configure_kdc
  echo "configuring db"
  configure_db
  echo "configuring hdfs"
  configure_hdfs
  echo "configuring client"
  configure_client
}

if [ "$1" == "kerberos" ]
  then
    echo "running kerberos docker compose"
    docker compose -f docker-compose-kerberos.yml up -d
    configure_containers
else
  echo "running non-kerberized docker compose"
  docker compose -f docker-compose.yml up -d
  docker exec docker_hdfs_1 cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
  docker exec docker_hdfs_1 cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
  docker exec docker_hdfs_1 /opt/hadoop/sbin/stop-dfs.sh
  docker exec docker_hdfs_1 /opt/hadoop/sbin/start-dfs.sh
  docker exec docker_vertica_1 vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"
  docker exec -it docker_client_1 /bin/bash
fi
