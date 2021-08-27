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

/opt/vertica/bin/vsql -U dbadmin -f /opt/vertica/packages/ParquetExport/ddl/install.sql
/opt/vertica/bin/vsql -U dbadmin -a << eof
ALTER DATABASE $DBNAME SET KerberosHostName = '${KHOST}';
ALTER DATABASE $DBNAME SET KerberosRealm = '${REALM}';
ALTER DATABASE $DBNAME SET KerberosKeytabFile = '${KTAB}';
CREATE USER user1;
GRANT ALL ON SCHEMA PUBLIC TO user1;
CREATE AUTHENTICATION kerberos METHOD 'gss' HOST '0.0.0.0/0';
ALTER AUTHENTICATION kerberos enable;
GRANT AUTHENTICATION kerberos TO user1;
CREATE AUTHENTICATION debug METHOD 'trust' HOST '0.0.0.0/0';
ALTER AUTHENTICATION debug enable;
GRANT AUTHENTICATION debug TO dbadmin;
GRANT ALL ON LIBRARY public.ParquetExportLib to user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExportFinalize() TO user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExportMulti() TO user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExport() TO user1;
eof

cp /keytabs/vertica.keytab /
chown dbadmin /vertica.keytab
chmod 600 /vertica.keytab

echo "Restarting Database to apply Kerberos settings."
sudo -u dbadmin /opt/vertica/bin/admintools -t stop_db -F -d $DBNAME
sudo -u dbadmin /opt/vertica/bin/admintools -t start_db -d $DBNAME

/opt/vertica/bin/vsql -U dbadmin -a -c "SELECT kerberos_config_check();"

mkdir -p /etc/pki/tls/certs/
cat /etc/hadoop/conf/hdfs.cert >> /etc/pki/tls/certs/ca-bundle.crt