#!/usr/bin/env bash

service ssh start

# Configure Kerberos
echo "[logging]
  default = FILE:/var/log/krb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
[libdefaults]
  default_realm = $REALM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
[realms]
  $REALM = {
    kdc = $KDC
    admin_server = $KDC
  }
[domain_realm]
  .example.com = $REALM
  example.com = $REALM" | tee /etc/krb5.conf

cp /keytabs/hdfs.keytab /root/.keytab

cp /hadoop/conf/core-site.xml /opt/hadoop/etc/hadoop/core-site.xml
cp /hadoop/conf/hdfs-site.xml /opt/hadoop/etc/hadoop/hdfs-site.xml
cp /hadoop/conf/ssl-server.xml /opt/hadoop/etc/hadoop/ssl-server.xml
cp /hadoop/conf/keystore /root/.keystore

export PATH=$PATH:/usr/bin

rm /hadoop/conf/hdfs.cert
keytool -delete -alias hdfs -keystore /root/.keystore -storepass password
keytool -genkey -keyalg RSA -alias hdfs -keystore /root/.keystore -validity 500 -keysize 2048 -dname "CN=hdfs.example.com, OU=hdfs, O=hdfs, L=hdfs, S=hdfs, C=hdfs" -no-prompt -storepass password -keypass password
echo "password" | keytool -export -alias hdfs -keystore /root/.keystore -rfc -file hdfs.cert
cp hdfs.cert /hadoop/conf/

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
