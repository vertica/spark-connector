@echo off

echo "running kerberos docker compose"
docker compose -f docker-compose-kerberos.yml up -d
echo "configuring kdc"
docker exec kdc /kdc/configure.sh
echo "configuring hdfs"
docker exec hdfs service ssh start
docker exec hdfs start-dfs.sh
docker exec -u 0 hdfs /hdfs-krb/kerberize.sh
docker exec hdfs stop-dfs.sh
docker exec hdfs start-dfs.sh
docker cp ../functional-tests/src/main/resources/3.1.1 docker_hdfs_1:/partitioned
docker exec docker_hdfs_1 hadoop fs -copyFromLocal /partitioned /3.1.1
echo "configuring client"
docker exec docker_krbclient_1 /client-krb/kerberize.sh
FOR /F "tokens=* USEBACKQ" %%F IN (`docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" hdfs`) DO (
	SET ip_address_var=%%F
)
docker exec vertica /bin/sh -c "echo %ip_address_var% hdfs.example.com hdfs | sudo tee -a /etc/hosts"
docker exec docker_krbclient_1 /bin/sh -c "echo %ip_address_var% hdfs.example.com hdfs | tee -a /etc/hosts"
echo "configuring db"
docker exec vertica /bin/sh -c "opt/vertica/bin/admintools -t create_db --database=docker --password='' --hosts=localhost"
docker exec vertica /bin/sh -c "sudo /usr/sbin/sshd -D"
docker exec -u 0 vertica /vertica-krb/kerberize.sh
docker exec -it docker_krbclient_1 /bin/bash
