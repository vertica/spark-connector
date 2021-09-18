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
echo "configuring client"
docker exec docker_krbclient_1 /client-krb/kerberize.sh
FOR /F "tokens=* USEBACKQ" %%F IN (`docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" hdfs`) DO (
	SET ip_address_var=%%F
)
docker exec vertica /bin/sh -c "echo %ip_address_var% hdfs.example.com hdfs | sudo tee -a /etc/hosts"
docker exec docker_krbclient_1 /bin/sh -c "echo %ip_address_var% hdfs.example.com hdfs | tee -a /etc/hosts"
echo "configuring db"
docker exec -u 0 vertica /vertica-krb/kerberize.sh
docker exec -it docker_krbclient_1 /bin/bash