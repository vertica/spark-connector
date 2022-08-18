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
    kdc = localhost
    admin_server = localhost
  }
[domain_realm]
  .example.com = $REALM
  example.com = $REALM" | tee /etc/krb5.conf

kdb5_util -P 'admin' create

systemctl start kadmin.service
systemctl start krb5kdc.service
chkconfig krb5kdc on
chkconfig kadmin on

# Create admin
$KADMIN -q "addprinc -pw admin admin/admin"
echo "*/admin@$REALM *" | tee -a /var/kerberos/krb5kdc/kadm5.acl

# Add user principals
for u in ${USERS//,/ };do
    $KADMIN -q "addprinc -pw ${u} ${u}"
done

$KADMIN -q "addprinc -randkey $V_PRINC"
$KADMIN -q "ktadd -norandkey -k vertica.keytab $V_PRINC"
chmod 777 vertica.keytab
cp vertica.keytab /keytabs

$KADMIN -q "addprinc -randkey $HDFS_PRINC"
$KADMIN	-q "addprinc -randkey $HTTP_HDFS_PRINC"
$KADMIN -q "ktadd -norandkey -k hdfs.keytab $HDFS_PRINC $HTTP_HDFS_PRINC"
chmod 777 hdfs.keytab
cp hdfs.keytab /keytabs

exec "$@"
