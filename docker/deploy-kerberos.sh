#!/usr/bin/env bash

docker compose -f docker-compose-kerberos.yml up -d

# Current Kerberos setup requires the containers to have an entry for the HDFS container in the /etc/hosts file
docker exec spark-connector-kerberos-vertica-1 /bin/sh -c "echo $(docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" spark-connector-kerberos-hdfs-1) hdfs.example.com hdfs | sudo tee -a /etc/hosts"
docker exec spark-connector-kerberos-client-1 /bin/sh -c "echo $(docker inspect -f "{{with index .NetworkSettings.Networks \"EXAMPLE.COM\"}}{{.IPAddress}}{{end}}" spark-connector-kerberos-hdfs-1) hdfs.example.com hdfs | tee -a /etc/hosts"
