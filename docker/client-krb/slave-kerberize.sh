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

cp /keytabs/worker.keytab /root/.keytab
cp /etc/hadoop/conf/* /hadoop-3.3.1/etc/hadoop/
keytool -import -file /hadoop-3.3.1/etc/hadoop/hdfs.cert -alias hdfs -keystore cacerts.jks -no-prompt -storepass password
echo 'user1' | kinit user1
