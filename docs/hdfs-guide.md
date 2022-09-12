# Setting up a single-node HDFS and using it with the Vertica Spark Connector

Here, we'll give some instructions for a simple one-node cluster setup on a Linux environment.

## 1. Download Hadoop

Navigate to the desired install location and download hadoop. You can replace version number with version of your choice:

```shell
wget https://httpd-mirror.sergal.org/apache/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz
```

## 2. Unzip and Change Permissions

Replace <hadoop_install> with desired hadoop install location.

```shell
mkdir <hadoop_install>/hadoop
sudo tar -zxvf hadoop-2.7.3.tar.gz -C <hadoop_install>/hadoop
cd <hadoop_install>/hadoop
sudo chmod 750 hadoop-2.9.2
```

## 3. Edit Hadoop Configuration

Edit etc/hadoop/hadoop-env.sh with the HADOOP_CONF_DIR variable to your directory. If necessary, you can also set the JAVA_HOME variable here

```shell
export HADOOP_CONF_DIR=${HADOOP_CONF_DIR:-"/<hadoop_install>/hadoop/hadoop-2.9.2/etc/hadoop"}
export JAVA_HOME=...
```

Edit etc/hadoop/core-site.xml with the following configuration (fill in your directory):

```shell
<configuration>
        <property>
                <name>fs.defaultFS</name>
                <value>hdfs://localhost:8020</value>
        </property>
        <property>
                <name>hadoop.tmp.dir</name>
                <value>/<hadoop_install>/hadoop/hadooptmpdata</value>
        </property>
</configuration>
```

and etc/hadoop/hdfs-site.xml with the following configuration (fill in your directory):

```shell
<configuration>
        <property>
                <name>dfs.replication</name>
                <value>1</value>
        </property>
        <property>
                <name>dfs.name.dir</name>
                <value>file://<hadoop_install>/hadoop/hdfs/namenode</value>
        </property>
        <property>
                <name>dfs.data.dir</name>
                <value>file://<hadoop_install>/hadoop/hdfs/datanode</value>
        </property>
        <property>
                <name>dfs.webhdfs.enabled</name>
                <value>true</value>
        </property>
</configuration>
```

Finally, set the HADOOP_HOME variable in your .bashrc (of whichever user is running hadoop):

```shell
export HADOOP_HOME=<hadoop_install>/hadoop/hadoop-2.9.2
```

## 4. Create directories

Create the directories referenced above:

```shell
cd /<hadoop_install>/hadoop/
mkdir hdfs
mkdir hadooptmpdata
```

## 5. Set up passwordless ssh to localhost:

```shell
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

and check that this worked:

```shell
ssh localhost
```

## 6. Format HDFS:

```shell
bin/hdfs namenode -format
```

## 7. Start HDFS

```shell
cd /scratch_b/<your username>/hadoop/hadoop-2.9.2
sbin/start-dfs.sh
```

## 8. Get Vertica to Work with HDFS

Each Vertica node needs to have access to a copy of the HDFS configuration. If these are on seperate machines, you can use a command such as rsync to copy the configuration over. This must be done for each Vertica node.

```shell
rsync -R --progress <hadoop_install>/hadoop/hadoop-2.9.2/etc/hadoop/hdfs-site.xml arehnby@eng-g9-158:/etc/hadoop/conf/
rsync -R --progress <hadoop_install>/hadoop/hadoop-2.9.2/etc/hadoop/core-site.xml arehnby@eng-g9-158:/etc/hadoop/conf/
```
