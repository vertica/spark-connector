# Kerberos Configuration User Guide

The general flow for setting up a Kerberized environment is as follows:

1. Create keytab files for services (Vertica and HDFS)

2. Create /etc/krb5.conf files for services

3. Kerberize services

On the client side:

1. Setup JAAS configuration file

2. Pass in Kerberos options to the connector

We will start with the service configuration. Click [here](#configuring-the-client) to go to the Client configuration guide. Note that the steps are meant to be followed in order.

## Configuring the KDC

Depending on whether you use Linux KDC or Active Directory as your KDC, you should follow one of the two following guides. In both guides, we assume that the Kerberos realm is example.com.

### Using Linux KDC

#### Install Kerberos packages

On CentOS, yum install krb5-server krb5-libs krb5-workstation (These packages will be different depending on which Linux distro you use)

#### Create the Kerberos configuration file

In the /etc folder, create a file named krb5.conf with the following contents:
```
[logging]
  default = FILE:/var/logkrb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
[libdefaults]
  default_realm = EXAMPLE.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
[realms]
  EXAMPLE.COM = {
    kdc = localhost
    admin_server = localhost
  }
[domain_realm]
  .example.com = EXAMPLE.COM
  example.com = EXAMPLE.COM
```

##### Note on ticket lifetimes and expiration

Neither Vertica or the connector will automatically renew Kerberos tickets. You should make sure the ticket lifetime is greater than a given Spark operation will take to complete, or have your own mechanism for renewing such tickets.

#### Create principals and keytabs

Run the following commands:
```
kdb5_util -P 'admin' create

systemctl start kadmin.service
systemctl start krb5kdc.service

# Create admin
kadmin.local -q "addprinc -pw admin admin/admin"
echo "*/admin@EXAMPLE.COM *" | tee -a /var/kerberos/krb5kdc/kadm5.acl

# Add user principal. Here, we have decided to create a user named user1 with password user1.
kadmin.local -q "addprinc -pw user1 user1"

kadmin.local -q "addprinc -randkey vertica/vertica.example.com@EXAMPLE.COM"
kadmin.local -q "ktadd -norandkey -k vertica.keytab vertica/vertica.example.com@EXAMPLE.COM"

# Create service principals and keytab files for Vertica and HDFS
kadmin.local -q "addprinc -randkey hdfs/hdfs.example.com@EXAMPLE.COM"
kadmin.local -q "ktadd -norandkey -k hdfs.keytab hdfs/hdfs.example.com@EXAMPLE.COM"
```
Before moving on to the next step, copy the newly created vertica.keytab file to the / directory on the Vertica host. Copy the hdfs.keytab file to the /root directory on the HDFS host.

### Using Active Directory as KDC

#### Create principals and keytabs

Run the following powershell script to create the Vertica and HDFS service users and keytab files:
```
# Remove vertica user if it exists already
dsrm "CN=vertica,CN=Users,DC=example,DC=com" -noprompt

# Create service user named vertica in the example.com realm
dsadd user "CN=vertica,CN=Users,DC=example,DC=com" -pwd $$V3rtic4$$

# Add the service principal name to a user by adding the ldap attribute to the cn of the user. The SPN will be vertica/vertica.example.com.
setspn -a vertica/vertica.example.com vertica

# Export keytab as vertica.keytab and map user
ktpass /princ vertica/vertica.example.com@EXAMPLE.COM /out vertica.keytab /mapuser example\vertica  /crypto ALL /pass $$V3rtic4$$ /ptype KRB5_NT_PRINCIPAL

# Remove hdfs user if it exists already
dsrm "CN=hdfs,CN=Users,DC=example,DC=com" -noprompt

# Create service user named hdfs in the example.com realm
dsadd user "CN=hdfs,CN=Users,DC=example,DC=com" -pwd $$V3rtic4$$

# Add the service principal name to a user by adding the ldap attribute to the cn of the user. The SPN will be hdfs/hdfs.example.com.
setspn -a hdfs/hdfs.example.com hdfs

# Export keytab as hdfs.keytab and map user
ktpass /princ hdfs/hdfs.example.com@EXAMPLE.COM /out hdfs.keytab /mapuser example\hdfs  /crypto ALL /pass $$V3rtic4$$ /ptype KRB5_NT_PRINCIPAL
```
Before moving on to the next step, copy the newly created vertica.keytab file to the / directory on the Vertica host. Copy the hdfs.keytab file to the /root directory on the HDFS host. Also, create a user principal for testing Kerberos connections later. Here, we have decided to create a user named "user1".

`dsadd user "CN=user1,CN=Users,DC=example,DC=com" -pwd $$V3rtic4$$`

## Kerberizing Vertica

### Create the Kerberos configuration file

In the /etc folder, create a file named krb5.conf with the following contents:
```
[logging]
  default = FILE:/var/log/krb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
[libdefaults]
  default_realm = EXAMPLE.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
[realms]
  EXAMPLE.COM = {
    kdc = kerberos.example.com
    admin_server = kerberos.example.com
  }
[domain_realm]
  .example.com = EXAMPLE.COM
  example.com = EXAMPLE.COM
```
Note that kerberos.example.com is the hostname of our KDC. Yours may be different, so change this accordingly.

### Set the Kerberos configuration parameters

Execute the following SQL commands in vsql:
```
ALTER DATABASE exampledb SET KerberosServiceName = 'vertica';
ALTER DATABASE exampledb SET KerberosHostName = 'vertica.example.com';
ALTER DATABASE exampledb SET KerberosRealm = 'EXAMPLE.COM';
ALTER DATABASE exampledb SET KerberosKeytabFile = '/vertica.keytab';
CREATE USER user1;
GRANT ALL ON SCHEMA PUBLIC TO user1;
CREATE AUTHENTICATION kerberos METHOD 'gss' HOST '0.0.0.0/0';
ALTER AUTHENTICATION kerberos enable;
GRANT AUTHENTICATION kerberos TO user1;
CREATE AUTHENTICATION debug METHOD 'trust' HOST '0.0.0.0/0';
ALTER AUTHENTICATION debug enable;
GRANT AUTHENTICATION debug TO dbadmin;
```
Restart the Vertica database:
```
/opt/vertica/bin/adminTools -t stop_db -d exampledb
/opt/vertica/bin/adminTools -t start_db -d exampledb
```
Make sure that the dbadmin user owns the keytab file: `chown dbadmin /vertica.keytab`

## Kerberizing HDFS

### Create the Kerberos configuration file

In the /etc folder, create a file named krb5.conf with the following contents:
```
[logging]
  default = FILE:/var/log/krb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
[libdefaults]
  default_realm = EXAMPLE.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
[realms]
  EXAMPLE.COM = {
    kdc = kerberos.example.com
    admin_server = kerberos.example.com
  }
[domain_realm]
  .example.com = EXAMPLE.COM
  example.com = EXAMPLE.COM
```

### Update HDFS configuration files

Update core-site.xml to set the following properties:
```
hadoop.security.authentication = kerberos

hadoop.security.authorization = truehadoop.security.auth_to_local =

RULE:[2:$1/$2@$0](.*/.*@EXAMPLE.COM)s/.*/root/

DEFAULT

hadoop.rpc.protection = authentication

hadoop.proxyuser.root.groups = *

hadoop.proxyuser.root.hosts = *

hadoop.proxyuser.superuser.hosts = *

hadoop.proxyuser.superuser.groups = *
```

Below is an example core-site.xml file that we have successfully tested Kerberos with:
```
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://hdfs.example.com:8020</value>
  </property>

  <property>
    <name>hadoop.security.authentication</name>
    <value>kerberos</value>
  </property>

  <property>
    <name>hadoop.security.authorization</name>
    <value>true</value>
  </property>

  <property>
    <name>hadoop.security.auth_to_local</name>
    <value>
	RULE:[2:$1/$2@$0](.*/.*@EXAMPLE.COM)s/.*/root/
	DEFAULT
    </value>
  </property>

  <property>
    <name>hadoop.rpc.protection</name>
    <value>authentication</value>
  </property>

  <property>
    <name>hadoop.proxyuser.root.groups</name>
    <value>*</value>
  </property>

  <property>
    <name>hadoop.proxyuser.root.hosts</name>
    <value>*</value>
  </property>

  <property>
    <name>hadoop.proxyuser.superuser.hosts</name>
    <value>*</value>
  </property>

  <property>
    <name>hadoop.proxyuser.superuser.groups</name>
    <value>*</value>
  </property>
</configuration>
```

Update hdfs-site.xml to set the following properties:
```
<configuration>
    <property>
        <name>dfs.replication</name>
        <value>1</value>
    </property>

    <property>
      <name>dfs.permissions</name>
      <value>false</value>
    </property>

    <property>
      <name>dfs.webhdfs.enabled</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.secondary.http.address</name>
      <value>hdfs.example.com:50090</value>
      <description>
        The secondary namenode http server address and port.
        If the port is 0 then the server will start on a free port.
      </description>
    </property>

    <property>
      <name>dfs.datanode.address</name>
      <value>hdfs.example.com:50010</value>
      <description>
        The address where the datanode server will listen to.
        If the port is 0 then the server will start on a free port.
      </description>
    </property>

    <property>
      <name>dfs.datanode.http.address</name>
      <value>hdfs.example.com:50075</value>
      <description>
        The datanode http server address and port.
        If the port is 0 then the server will start on a free port.
      </description>
    </property>

    <property>
      <name>dfs.data.transfer.protection</name>
      <value>authentication</value>
    </property>

    <property>
      <name>dfs.http.policy</name>
      <value>HTTPS_ONLY</value>
    </property>

    <property>
      <name>dfs.http.address</name>
      <value>hdfs.example.com:50070</value>
      <description>
        The address and the base port where the dfs namenode web ui will listen on.
        If the port is 0 then the server will start on a free port.
      </description>
    </property>

    <property>
      <name>dfs.namenode.https-address</name>
      <value>hdfs.example.com:50070</value>
      <description>
        The address and the base port where the dfs namenode web ui will listen on.
        If the port is 0 then the server will start on a free port.
      </description>
    </property>

    <property>
      <name>dfs.namenode.rpc-bind-host</name>
      <value>hdfs</value>
      <description>
        The actual address the RPC server will bind to. If this optional address is
        set, it overrides only the hostname portion of dfs.namenode.rpc-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node listen on all interfaces by
        setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.servicerpc-bind-host</name>
      <value>hdfs</value>
      <description>
        The actual address the service RPC server will bind to. If this optional address is
        set, it overrides only the hostname portion of dfs.namenode.servicerpc-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node listen on all interfaces by
        setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.http-bind-host</name>
      <value>hdfs</value>
      <description>
        The actual adress the HTTP server will bind to. If this optional address
        is set, it overrides only the hostname portion of dfs.namenode.http-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node HTTP server listen on all
        interfaces by setting it to 0.0.0.0.
      </description>
    </property>

    <property>
      <name>dfs.namenode.https-bind-host</name>
      <value>hdfs</value>
      <description>
        The actual adress the HTTPS server will bind to. If this optional address
        is set, it overrides only the hostname portion of dfs.namenode.https-address.
        It can also be specified per name node or name service for HA/Federation.
        This is useful for making the name node HTTPS server listen on all
        interfaces by setting it to 0.0.0.0.
      </description>
    </property>

    <!-- Enable NFS -->
    <!-- https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsNfsGateway.html -->
    <property>
      <name>dfs.namenode.accesstime.precision</name>
      <value>3600000</value>
      <description>
        The access time for HDFS file is precise up to this value.
        The default value is 1 hour. Setting a value of 0 disables
        access times for HDFS.
      </description>
    </property>

    <property>
      <name>dfs.datanode.data.dir.perm</name>
      <value>700</value>
    </property>

    <property>
      <name>dfs.namenode.kerberos.principal</name>
      <value>hdfs/hdfs.example.com@EXAMPLE.COM</value>
    </property>

    <property>
      <name>dfs.namenode.keytab.file</name>
      <value>/root/hdfs.keytab</value>
    </property>

    <property>
      <name>dfs.web.authentication.kerberos.principal</name>
      <value>hdfs/hdfs.example.com@EXAMPLE.COM</value>
    </property>

    <property>
      <name>dfs.web.authentication.keytab.file</name>
      <value>/root/hdfs.keytab</value>
    </property>

    <property>
      <name>dfs.datanode.kerberos.principal</name>
      <value>hdfs/hdfs.example.com@EXAMPLE.COM</value>
    </property>

    <property>
      <name>dfs.datanode.keytab.file</name>
      <value>/root/hdfs.keytab</value>
    </property>

    <property>
      <name>dfs.block.access.token.enable</name>
      <value>true</value>
    </property>

    <property>
      <name>dfs.nfs3.dump.dir</name>
      <value>/data/hdfs-nfs/</value>
    </property>

    <property>
      <name>dfs.nfs.exports.allowed.hosts</name>
      <value>* rw</value>
    </property>

    <property>
        <name>dfs.client.use.datanode.hostname</name>
        <value>true</value>
    </property>

    <property>
        <name>dfs.datanode.use.datanode.hostname</name>
        <value>true</value>
    </property>
</configuration>
```

## Configuring HDFS as SSL server and Vertica as SSL client

In order for Vertica to communicate with HDFS, you will need to make HDFS an SSL server and Vertica an SSL client.

On the HDFS host, in the same directory as the other HDFS configuration files, create/update ssl-server.xml with the following properties:
```
<configuration>
    <property>
        <name>ssl.server.keystore.keypassword</name>
        <value>password</value>
    </property>

    <property>
        <name>ssl.server.keystore.password</name>
        <value>password</value>
    </property>

    <property>
        <name>ssl.server.keystore.location</name>
        <value>/root/.keystore</value>
    </property>
</configuration>
```

Still on the HDFS host, run the following commands:
```
keytool -genkey -keyalg RSA -alias hdfs -keystore /root/.keystore -validity 500 -keysize 2048
keytool -export -alias hdfs -keystore /root/.keystore -rfc -file hdfs.cert
```

Copy hdfs.cert to the / directory on the Vertica host.

On the Vertica host, run the following commands:
```
mkdir -p /etc/pki/tls/certs/
cp /etc/ssl/certs/ca-certificates.crt /etc/pki/tls/certs/ca-bundle.crt
cat /hdfs.cert >> /etc/pki/tls/certs/ca-bundle.crt
```

## Configuring the client

### Create the Kerberos configuration file

In the /etc folder, create a file named krb5.conf with the following contents:
```
[logging]
  default = FILE:/var/log/krb5libs.log
  kdc = FILE:/var/log/krb5kdc.log
  admin_server = FILE:/var/log/kadmind.log
[libdefaults]
  default_realm = EXAMPLE.COM
  dns_lookup_realm = false
  dns_lookup_kdc = false
  ticket_lifetime = 24h
  renew_lifetime = 7d
  forwardable = true
[realms]
  EXAMPLE.COM = {
    kdc = kerberos.example.com
    admin_server = kerberos.example.com
  }
[domain_realm]
  .example.com = EXAMPLE.COM
  example.com = EXAMPLE.COM
```

### Set path to JAAS configuration file

Add `-Djava.security.auth.login.config=/jaas.config` to your JAVA_OPTS environment variable.

### Creating the JAAS configuration file

Create the JAAS config in the / directory (the path specified in the previous step):
```
Client {
  com.sun.security.auth.module.Krb5LoginModule required
  useKeyTab=false
  useTicketCache=true
  doNotPrompt=true;
};
```

## Testing the Kerberos configuration

### Getting the TGT

Since we specified in the JAAS config to use the ticket cache, we will need to run kinit user1. When prompted for the password, enter "user1", as we specified earlier when creating the user principal.

### Running the application

Run your client application code passing in the following options to Spark:
```
user="user1"
kerberos_service_name="vertica"
kerberos_host_name="vertica.example.com"
jaas_config_name="Client"
```
Note: This is not a complete list. You will still need to pass in the host name, db, port, etc.

If Kerberos is properly configured, then you'll be able to successfully read/write from/to Vertica without needing to provide a password.
