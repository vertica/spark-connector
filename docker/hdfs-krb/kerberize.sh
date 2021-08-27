#!/usr/bin/env bash

echo "[logging]
 default = FILE:/var/log/krb5libs.log
 kdc = FILE:/var/log/krb5kdc.log
 admin_server = FILE:/var/log/kadmind.log
[libdefaults]
 default_realm = $REALM
 dns_lookup_realm = false
 dns_lookup_kdc = false
 ticket_lifetime = 24h
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
