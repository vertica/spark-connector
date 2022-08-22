#!/bin/bash

# This file has been modified for use by the Spark Connector
# See original entrypoint script at https://github.com/vertica/vertica-kubernetes/blob/main/docker-vertica/docker-entrypoint.sh

set -e

start_cron(){
    # daemonizes, no need for &
    sudo /usr/sbin/cron
}

# We copy back the files normally stored in /opt/vertica/config/.  We do this
# because we have a Persistent Volume that backs /opt/vertica/config, so
# it starts up empty and must be populated
copy_config_files() {
    mkdir -p /opt/vertica/config/licensing

    mv /home/dbadmin/logrotate/* /opt/vertica/config/ 2>/dev/null || true

    cp -r /home/dbadmin/licensing/ce/* /opt/vertica/config/licensing 2>/dev/null || true
    chmod -R ugo+r,u+rw /opt/vertica/config/licensing
}

# Ensure all PV paths are owned by dbadmin.  This is done for some PVs that
# start with restrictive ownership.
ensure_path_is_owned_by_dbadmin() {
    # -z is to needed in case input arg is empty
    [ -z "$1" ] || [ "$(stat -c "%U" "$1")" == "dbadmin" ] || sudo chown -R dbadmin:verticadba "$1"
}

start_cron
ensure_path_is_owned_by_dbadmin /opt/vertica/config
ensure_path_is_owned_by_dbadmin /opt/vertica/log
ensure_path_is_owned_by_dbadmin $DATA_PATH
ensure_path_is_owned_by_dbadmin $DEPOT_PATH
copy_config_files

# Create database
/opt/vertica/bin/admintools -t list_db --database=docker || \
    /opt/vertica/bin/admintools -t create_db --database="${VERTICA_DATABASE:-docker}" --password="${VERTICA_PASSWORD}" --hosts=localhost

# Start database
if [ "$(/opt/vertica/bin/admintools -t db_status --status=DOWN)" == "${VERTICA_DATABASE:-docker}" ]; then
    /opt/vertica/bin/admintools -t start_db --database="${VERTICA_DATABASE:-docker}" --password="${VERTICA_PASSWORD}" --hosts=localhost
fi

# Configure database
/opt/vertica/bin/vsql -c "ALTER DATABASE docker SET MaxClientSessions=100;"

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
  example.com = $REALM" | sudo tee /etc/krb5.conf

/opt/vertica/bin/vsql -U ${VERTICA_DATABASE:-dbadmin} -f /opt/vertica/packages/ParquetExport/ddl/install.sql
/opt/vertica/bin/vsql -U ${VERTICA_DATABASE:-dbadmin} -a << EOF
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
GRANT AUTHENTICATION debug TO ${VERTICA_DATABASE:-dbadmin};
GRANT ALL ON LIBRARY public.ParquetExportLib to user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExportFinalize() TO user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExportMulti() TO user1;
GRANT EXECUTE ON TRANSFORM FUNCTION public.ParquetExport() TO user1;
EOF

sudo cp /keytabs/vertica.keytab /
sudo chown ${VERTICA_DATABASE:-dbadmin} /vertica.keytab
sudo chmod 600 /vertica.keytab

echo "Restarting Database to apply Kerberos settings."
sudo -u ${VERTICA_DATABASE:-dbadmin} /opt/vertica/bin/admintools -t stop_db -F -d $DBNAME
sudo -u ${VERTICA_DATABASE:-dbadmin} /opt/vertica/bin/admintools -t start_db -d $DBNAME

/opt/vertica/bin/vsql -U ${VERTICA_DATABASE:-dbadmin} -a -c "SELECT kerberos_config_check();"

sudo mkdir -p /etc/pki/tls/certs/
cat /etc/hadoop/conf/hdfs.cert | sudo tee -a /etc/pki/tls/certs/ca-bundle.crt

echo "Vertica container is now running"

sudo /usr/sbin/sshd -D
