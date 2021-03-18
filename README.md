# Vertica Spark Connector

This component acts as a bridge between Spark and Vertica, allowing the user to either retrieve data from Vertica for processing in Spark, or store processed data from Spark into Vertica.

Why is this connector desired instead of using a more generic JDBC connector? A few reasons:
* Vertica-specific syntax and features. This connector can support things like Vertica projections
* Authentication to the Vertica server 
* Segmentation. We can use the way that Vertica segments data to inform our operations. 
* Ability to use other technology as an intermediary for data transfer. This is necessary for maximizing performance of parallel loads.


This connector is built as a JAR file to be sourced in your spark application. 

The connector relies on a distributed filesystem, such as HDFS, to act as a bridge between Spark and Vertica. This is done to allow for maximum performance at high scale. 

![Overview](img/Overview.png?raw=true "")

## Getting Started

To get started with using the connector, we'll need to make sure all the prerequisites are in place. These are:
- A Vertica installation
- An HDFS cluster, for use as an intermediary between Spark and Vertica
- A spark application, either running locally for quick testing, or running on a spark cluster.

### Vertica 

Follow the [Vertica Documenation](https://www.vertica.com/docs/10.0.x/HTML/Content/Authoring/InstallationGuide/Other/InstallationGuide.htm) for steps on installing Vertica.

### Spark

There are several examples of spark programs that use this connector in the [examples](/examples) directory. 

The methods for creating a spark cluster are documented [here](https://spark.apache.org/docs/latest/cluster-overview.html).

Once you have a spark cluster, you can run such an application with spark-submit, including the connector jar.

```shell
spark-submit --master spark://cluster-url.com:7077 --deploy-mode cluster sparkconnectorprototype-assembly-0.1.jar
```


### HDFS Cluster

An HDFS setup can have various configurations, for details of what might best fit your infrastructure the [HDFS Architecture Guide](https://hadoop.apache.org/docs/r1.2.1/hdfs_design.html) is recommended.

Here, we'll give some instructions for a simple one-node cluster setup on a linux environment.

#### 1. Download Hadoop

Navigate to the desired install location and download hadoop. You can replace version number with version of your choice:

```shell
wget https://httpd-mirror.sergal.org/apache/hadoop/common/hadoop-2.9.2/hadoop-2.9.2.tar.gz
```

#### 2. Unzip and Change Permissions

Replace <hadoop_install> with desired hadoop install location.

```shell
mkdir <hadoop_install>/hadoop
sudo tar -zxvf hadoop-2.7.3.tar.gz -C <hadoop_install>/hadoop
cd <hadoop_install>/hadoop
sudo chmod 750 hadoop-2.9.2
```

#### 3. Edit Hadoop Configuration

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


#### 4. Create directories

Create the directories referenced above:

```shell
cd /<hadoop_install>/hadoop/
mkdir hdfs
mkdir hadooptmpdata
```

#### 5. Set up passwordless ssh to localhost:

```shell
cat ~/.ssh/id_rsa.pub >> ~/.ssh/authorized_keys
```

and check that this worked:

```shell
ssh localhost
```

#### 6. Format HDFS:

```shell
bin/hdfs namenode -format
```

#### 7. Start HDFS

```shell
cd /scratch_b/<your username>/hadoop/hadoop-2.9.2
sbin/start-dfs.sh
```

#### 8. Get Vertica to Work with HDFS

Each Vertica node needs to have access to a copy of the HDFS configuration. If these are on seperate machines, you can use a command such as rsync to copy the configuration over. This must be done for each Vertica node.

```shell
rsync -R --progress <hadoop_install>/hadoop/hadoop-2.9.2/etc/hadoop/hdfs-site.xml arehnby@eng-g9-158:/etc/hadoop/conf/
rsync -R --progress <hadoop_install>/hadoop/hadoop-2.9.2/etc/hadoop/core-site.xml arehnby@eng-g9-158:/etc/hadoop/conf/
```


## Connector Usage

Using the connector in Spark is straightforward. It requires the data source name, an options map, and if writing to Vertica, a [Spark Save Mode](https://spark.apache.org/docs/2.2.0/api/java/index.html?org/apache/spark/sql/SaveMode.html).


```scala
val opts = Map(
    "host" -> "vertica_hostname",
    "user" -> "vertica_user",
    "db" -> "db_name",
    "password" -> "db_password",
    "staging_fs_url" -> "hdfs://hdfs-url:7077/data",
    "logging_level" -> "ERROR",
    "table" -> "tablename"
  )

// Create data and put it in a dataframe
val schema = new StructType(Array(StructField("col1", IntegerType)))
val data = Seq(Row(77))
val df = spark.createDataFrame(spark.sparkContext.parallelize(data), schema).coalesce(1)

val mode = SaveMode.ErrorIfExists

// Write the dataframe to Vertica
df.write.format("com.vertica.spark.datasource.VerticaSource").options(opts ).mode(mode).save()

// Read it back from Vertica
val dfRead: DataFrame = spark.read.format("com.vertica.spark.datasource.VerticaSource").options(opts).load()
```

Below is a detailed list of connector options:

| Configuration Option                           | Possible Values                                    | Description                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | Required                                                           | Default Value               |
|------------------------------------------------|----------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------|-----------------------------|
| table                                          | String                                             | The name of the target Vertica table to save your Spark DataFrame. New: Should parse schema from tablename if specified with schema.table                                                                                                                                                                                                                                                                                                                                                                                                                      | No (Either or with Query)                                          |                             |
| query                                          | String                                             | Alternative to specifying the table name. This is any SQL query that will produce results. The user can use this to specify a join between two tables, or an aggregation, for example.                                                                                                                                                                                                                                                                                                                                                                         | No (Either or with Table)                                          |                             |
| db                                             | String                                             | The name of the Vertica Database                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               | Yes                                                                |                             |
| user                                           | String                                             | The name of the Vertica user. This user must have CREATE and INSERT privileges in the Vertica schema. The schema defaults to “public”, but may be changed using the “dbschema” optional tuning parameter.                                                                                                                                                                                                                                                                                                                                                      | No if SSO, unless specifying the Kerberos user (when using kinit?) |                             |
| password                                       | String                                             | The password for the Vertica user.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             | No if SSO                                                          |                             |
| host                                           | String                                             | The hostname of a Vertica node. This value is used to make the initial connection to Vertica and look up all the other Vertica node IP addresses. You can provide a specific IP address or a resolvable name such as myhostname.com.                                                                                                                                                                                                                                                                                                                           | Yes                                                                |                             |
| dbschema                                       | Public (+ some number of currently unknown values) | The schema space for the Vertica table.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        | No                                                                 | public                      |
| port                                           | Int                                                | The Vertica Port.                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                              | No                                                                 | 5433                        |
| failed_rows_percent_tolerance                  | Number                                             | The tolerance level for failed rows, as a percentage. For example, to specify that the job fails if greater than 10% of the rows are rejected, specify this value as 0.10 for 10% tolerance.                                                                                                                                                                                                                                                                                                                                                                   | No                                                                 | 0.00                        |
| strlen                                         | Int                                                | The string length. Use this option to increase (or decrease) the default length when saving Spark StringType to Vertica VARCHAR type. Used to determine whether to use VARCHAR or LONG VARCHAR                                                                                                                                                                                                                                                                                                                                                                 | No                                                                 | 1024                        |
| target_table_sql (previously target_table_ddl) | String                                             | A SQL statement to run in Vertica before copying data. You usually use this option to create the target table to receive the data from Spark. The table this statement creates should match the table name you specify in the table option. See Creating the Destination Table From Spark for more information.                                                                                                                                                                                                                                                | No                                                                 |                             |
| copy_column_list                               | String                                             | A custom column list to supply to the Vertica COPY statement that loads the Spark data into Vertica. This option lets you adjust the data so that the Vertica table and the Spark data frame do not have to have the same number of columns. See Copying Data to a Table With a Different Schema for more information.                                                                                                                                                                                                                                         | No                                                                 |                             |
| logging_level                                  | String -> Enum                                     | The logging level used in the connector. One of “ERROR”, “WARNING”, “INFO”, or “DEBUG”                                                                                                                                                                                                                                                                                                                                                                                                                                                                         | No                                                                 | ERROR                       |
| num_partitions (previously numpartitions)      | Int                                                | The number of Spark partitions used when reading from Vertica. Each of these will correspond to a task with its own JDBC connection. Performance testing has indicated that the ideal number of partitions is 4*N where N is the number nodes in the Vertica cluster for the direct read method. Most efficient partition count for intermediate method may vary, will require testing and may be a tradeoff of memory vs time.                                                                                                                                | No                                                                 | 1 per exported parquet file |
| staging_fs_url (previously hdfs_url)           | URL String                                         | The fully-qualified path to a directory in HDFS or S3 that will be used as a data staging area. For example, hdfs://myhost:8020/data/test. The connector first saves the DataFrame in its entirety to this location before loading into Vertica. The data is saved in ORC format, and the files are saved in a directory specific to each job. This directory is then is deleted when the job completes. Note that you need additional configuration changes for to use HDFS. You must first configure all nodes to use HDFS. See Configuring the hdfs Scheme. | Yes                                                                |                             |
| kerberos_service_name                          | String                                             | The Kerberos service name, as specified when creating the service principal (outlined here)                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | No (Yes if using Kerberos)                                         |                             |
| kerberos_host_name                             | String                                             | The Kerberos host name, as specified when creating the service principal (outlined here)                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | No (Yes if using Kerberos)                                         |                             |
| jaas_config_name                               | String                                             | The name of the JAAS configuration used for Kerberos authentication                                                                                                                                                                                                                                                                                                                                                                                                                                                                                            | No                                                                 | verticajdbc                 |
| ssl                                            | Bool                                               | When set to true, connections with Vertica are encrypted                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       | No                                                                 | false                       |
| key_store_path                                 | String                                             | The local path to a .JKS file containing your private keys and their corresponding certificate chains.                                                                                                                                                                                                                                                                                                                                                                                                                                                         | No                                                                 |                             |
| key_store_password                             | String                                             | The password protecting the keystore file. If individual keys are also encrypted, the keystore file password must match the password for a key within the keystore.                                                                                                                                                                                                                                                                                                                                                                                            | No                                                                 |                             |
| trust_store_path                               | String                                             | The local path to a .JKS truststore file containing certificates from authorities you trust.                                                                                                                                                                                                                                                                                                                                                                                                                                                                   | No                                                                 |                             |
| trust_store_password                           | String                                             | The password protecting the truststore file                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                    | No                                                                 |                             |

## Limitations

The connector supports basic Spark types. Complex types are not currently supported.
