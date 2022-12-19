# Troubleshooting Guide

- [Debug Logs](#debug-logs)
   * [Log Levels](#log-levels)
   * [Spark Logs](#spark-logs)
   * [Vertica Logs](#vertica-logs)
- [Timed Operations](#timed-operations)

## Debug Logs

The Spark Connector outputs various logging information during a Spark job. The intention of this document is to break down some of these logs as well as describe how to increase the logging level or switch on certain metrics. 

### Log Levels

The Spark Connector uses the Scala-Logging library which wraps SLF4J. As such the logging levels are the same as Java's LOG4J and each tier includes the levels before it, as shown below.

| Property Value | Log Levels |
|--------|-------------|
| `FATAL` | FATAL |
| `ERROR` | FATAL, ERROR |
| `WARNING` | WARNING, ERROR, FATAL |
| `INFO` | INFO, WARNING, ERROR, FATAL|
| `DEBUG` | DEBUG, INFO, WARNING, ERROR, FATAL|
| `TRACE` | TRACE, DEBUG, INFO, WARNING, ERROR, FATAL|
| `ALL` | TRACE, DEBUG, INFO, WARNING, ERROR, FATAL|
| `OFF` | None |

These log levels are written into the connector in various places and we have the option of adjusting the property value in order to obtain more information about what the connector is doing.

### Spark Logs

As shown below, when building a new SparkSession we can also adjust the logging level by using one of the methods that belongs to the SparkSession class.

```
 val spark = SparkSession.builder()
      .appName("Vertica-Spark Connector Scala Example")
      .getOrCreate()
 spark.sparkContext.setLogLevel("DEBUG") // this line sets our logging level to DEBUG 
```

The following is an an example of what the logging output during a simple write then read job would look like.

<details>
  <summary>
    Log Output
  </summary>

```
root@fcd239af6c6b:/spark-connector/examples/scala# ./submit-examples.sh writeThenRead
22/12/13 17:45:22 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
22/12/13 17:45:22 INFO SparkContext: Running Spark version 3.3.0
22/12/13 17:45:22 INFO ResourceUtils: ==============================================================
22/12/13 17:45:22 INFO ResourceUtils: No custom resources configured for spark.driver.
22/12/13 17:45:22 INFO ResourceUtils: ==============================================================
22/12/13 17:45:22 INFO SparkContext: Submitted application: Vertica-Spark Connector Scala Example
22/12/13 17:45:22 INFO ResourceProfile: Default ResourceProfile created, executor resources: Map(cores -> name: cores, amount: 1, script: , vendor: , memory -> name: memory, amount: 1024, script: , vendor: , offHeap -> name: offHeap, amount: 0, script: , vendor: ), task resources: Map(cpus -> name: cpus, amount: 1.0)
22/12/13 17:45:22 INFO ResourceProfile: Limiting resource is cpu
22/12/13 17:45:22 INFO ResourceProfileManager: Added ResourceProfile id: 0
22/12/13 17:45:23 INFO SecurityManager: Changing view acls to: root
22/12/13 17:45:23 INFO SecurityManager: Changing modify acls to: root
22/12/13 17:45:23 INFO SecurityManager: Changing view acls groups to: 
22/12/13 17:45:23 INFO SecurityManager: Changing modify acls groups to: 
22/12/13 17:45:23 INFO SecurityManager: SecurityManager: authentication disabled; ui acls disabled; users  with view permissions: Set(root); groups with view permissions: Set(); users  with modify permissions: Set(root); groups with modify permissions: Set()
22/12/13 17:45:23 INFO Utils: Successfully started service 'sparkDriver' on port 39771.
22/12/13 17:45:23 INFO SparkEnv: Registering MapOutputTracker
22/12/13 17:45:23 INFO SparkEnv: Registering BlockManagerMaster
22/12/13 17:45:23 INFO BlockManagerMasterEndpoint: Using org.apache.spark.storage.DefaultTopologyMapper for getting topology information
22/12/13 17:45:23 INFO BlockManagerMasterEndpoint: BlockManagerMasterEndpoint up
22/12/13 17:45:23 INFO SparkEnv: Registering BlockManagerMasterHeartbeat
22/12/13 17:45:23 INFO DiskBlockManager: Created local directory at /tmp/blockmgr-c4bacaea-273c-4cd8-a939-bc016d063770
22/12/13 17:45:23 INFO MemoryStore: MemoryStore started with capacity 1048.8 MiB
22/12/13 17:45:23 INFO SparkEnv: Registering OutputCommitCoordinator
22/12/13 17:45:23 INFO Utils: Successfully started service 'SparkUI' on port 4040.
22/12/13 17:45:23 INFO SparkContext: Added JAR file:/spark-connector/examples/scala/target/scala-2.12/vertica-spark-scala-examples.jar at spark://fcd239af6c6b:39771/jars/vertica-spark-scala-examples.jar with timestamp 1670953522913
22/12/13 17:45:23 INFO StandaloneAppClient$ClientEndpoint: Connecting to master spark://spark:7077...
22/12/13 17:45:23 INFO TransportClientFactory: Successfully created connection to spark/172.19.0.6:7077 after 26 ms (0 ms spent in bootstraps)
22/12/13 17:45:23 INFO StandaloneSchedulerBackend: Connected to Spark cluster with app ID app-20221213174523-0000
22/12/13 17:45:23 INFO Utils: Successfully started service 'org.apache.spark.network.netty.NettyBlockTransferService' on port 41747.
22/12/13 17:45:23 INFO NettyBlockTransferService: Server created on fcd239af6c6b:41747
22/12/13 17:45:23 INFO BlockManager: Using org.apache.spark.storage.RandomBlockReplicationPolicy for block replication policy
22/12/13 17:45:24 INFO BlockManagerMaster: Registering BlockManager BlockManagerId(driver, fcd239af6c6b, 41747, None)
22/12/13 17:45:24 INFO BlockManagerMasterEndpoint: Registering block manager fcd239af6c6b:41747 with 1048.8 MiB RAM, BlockManagerId(driver, fcd239af6c6b, 41747, None)
22/12/13 17:45:24 INFO BlockManagerMaster: Registered BlockManager BlockManagerId(driver, fcd239af6c6b, 41747, None)
22/12/13 17:45:24 INFO BlockManager: Initialized BlockManager: BlockManagerId(driver, fcd239af6c6b, 41747, None)
22/12/13 17:45:24 INFO StandaloneAppClient$ClientEndpoint: Executor added: app-20221213174523-0000/0 on worker-20221213173342-172.19.0.2-34321 (172.19.0.2:34321) with 1 core(s)
22/12/13 17:45:24 INFO StandaloneSchedulerBackend: Granted executor ID app-20221213174523-0000/0 on hostPort 172.19.0.2:34321 with 1 core(s), 1024.0 MiB RAM
22/12/13 17:45:24 INFO StandaloneAppClient$ClientEndpoint: Executor updated: app-20221213174523-0000/0 is now RUNNING
22/12/13 17:45:24 INFO StandaloneSchedulerBackend: SchedulerBackend is ready for scheduling beginning after reached minRegisteredResourcesRatio: 0.0
------------------------------------
-
- EXAMPLE: write data into Vertica then read it back 
-
------------------------------------
22/12/13 17:45:24 INFO SharedState: Setting hive.metastore.warehouse.dir ('null') to the value of spark.sql.warehouse.dir.
22/12/13 17:45:24 INFO SharedState: Warehouse path is 'file:/spark-connector/examples/scala/spark-warehouse'.
22/12/13 17:45:26 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Registered executor NettyRpcEndpointRef(spark-client://Executor) (172.19.0.2:44022) with ID 0,  ResourceProfileId 0
22/12/13 17:45:26 INFO BlockManagerMasterEndpoint: Registering block manager 172.19.0.2:39883 with 434.4 MiB RAM, BlockManagerId(0, 172.19.0.2, 39883, None)
[col1: int]
22/12/13 17:45:27 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:27 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:27 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:27 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:27 INFO VerticaJdbcLayer: Connecting to Vertica with URI: jdbc:vertica://vertica:5433/docker
22/12/13 17:45:27 INFO VerticaJdbcLayer: main: Successfully connected to Vertica.
22/12/13 17:45:28 INFO VerticaJdbcLayer: Connecting to Vertica with URI: jdbc:vertica://vertica:5433/docker
22/12/13 17:45:28 INFO VerticaJdbcLayer: main: Successfully connected to Vertica.
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:28 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:28 INFO VerticaDistributedFilesystemWritePipe: Writing data to Parquet file.
22/12/13 17:45:28 INFO TableUtils: BUILDING TABLE WITH COMMAND: Right(CREATE table "dftest" ("col1" INTEGER) INCLUDE SCHEMA PRIVILEGES )
22/12/13 17:45:31 INFO CodeGenerator: Code generated in 157.763919 ms
22/12/13 17:45:31 INFO OverwriteByExpressionExec: Start processing data source write support: com.vertica.spark.datasource.v2.VerticaBatchWrite@57fe6f2d. The input RDD has 1 partitions.
22/12/13 17:45:31 INFO SparkContext: Starting job: save at BasicReadWriteExamples.scala:80
22/12/13 17:45:31 INFO DAGScheduler: Got job 0 (save at BasicReadWriteExamples.scala:80) with 1 output partitions
22/12/13 17:45:31 INFO DAGScheduler: Final stage: ResultStage 0 (save at BasicReadWriteExamples.scala:80)
22/12/13 17:45:31 INFO DAGScheduler: Parents of final stage: List()
22/12/13 17:45:31 INFO DAGScheduler: Missing parents: List()
22/12/13 17:45:31 INFO DAGScheduler: Submitting ResultStage 0 (CoalescedRDD[3] at save at BasicReadWriteExamples.scala:80), which has no missing parents
22/12/13 17:45:31 INFO MemoryStore: Block broadcast_0 stored as values in memory (estimated size 20.6 KiB, free 1048.8 MiB)
22/12/13 17:45:31 INFO MemoryStore: Block broadcast_0_piece0 stored as bytes in memory (estimated size 10.2 KiB, free 1048.8 MiB)
22/12/13 17:45:31 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on fcd239af6c6b:41747 (size: 10.2 KiB, free: 1048.8 MiB)
22/12/13 17:45:31 INFO SparkContext: Created broadcast 0 from broadcast at DAGScheduler.scala:1513
22/12/13 17:45:31 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 0 (CoalescedRDD[3] at save at BasicReadWriteExamples.scala:80) (first 15 tasks are for partitions Vector(0))
22/12/13 17:45:31 INFO TaskSchedulerImpl: Adding task set 0.0 with 1 tasks resource profile 0
22/12/13 17:45:31 INFO TaskSetManager: Starting task 0.0 in stage 0.0 (TID 0) (172.19.0.2, executor 0, partition 0, PROCESS_LOCAL, 5241 bytes) taskResourceAssignments Map()
22/12/13 17:45:31 INFO BlockManagerInfo: Added broadcast_0_piece0 in memory on 172.19.0.2:39883 (size: 10.2 KiB, free: 434.4 MiB)
22/12/13 17:45:34 INFO TaskSetManager: Finished task 0.0 in stage 0.0 (TID 0) in 3419 ms on 172.19.0.2 (executor 0) (1/1)
22/12/13 17:45:34 INFO TaskSchedulerImpl: Removed TaskSet 0.0, whose tasks have all completed, from pool 
22/12/13 17:45:34 INFO DAGScheduler: ResultStage 0 (save at BasicReadWriteExamples.scala:80) finished in 3.631 s
22/12/13 17:45:34 INFO DAGScheduler: Job 0 is finished. Cancelling potential speculative or zombie tasks for this job
22/12/13 17:45:34 INFO TaskSchedulerImpl: Killing all running tasks in stage 0: Stage finished
22/12/13 17:45:34 INFO DAGScheduler: Job 0 finished: save at BasicReadWriteExamples.scala:80, took 3.664706 s
22/12/13 17:45:34 INFO OverwriteByExpressionExec: Data source write support com.vertica.spark.datasource.v2.VerticaBatchWrite@57fe6f2d is committing.
22/12/13 17:45:34 INFO VerticaJdbcLayer: Kerberos is not enabled in the hadoop config.
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set AWSAuth
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set AWSRegion
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set AWSSessionToken
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set AWSEndpoint
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set AWSEnableHttps
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not set S3EnableVirtualAddressing
22/12/13 17:45:34 INFO VerticaJdbcLayer: Did not setup GCS authentications
22/12/13 17:45:34 INFO VerticaDistributedFilesystemWritePipe: Building default copy column list
22/12/13 17:45:34 INFO SchemaTools: Load by name. Column list: ("col1")
22/12/13 17:45:34 INFO VerticaDistributedFilesystemWritePipe: The copy statement is: 
COPY "dftest" ("col1") FROM 'webhdfs://hdfs:50070/data/bb2e6fe9_c72a_4c10_af81_7d7a00fbadad/*.parquet' ON ANY NODE parquet REJECTED DATA AS TABLE "dftest_bb2e6fe9_c72a_4c10_af81_7d7a00fbadad_COMMITS" NO COMMIT
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Performing copy from file store to Vertica
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Checking number of rejected rows via statement: SELECT COUNT(*) as count FROM "dftest_bb2e6fe9_c72a_4c10_af81_7d7a00fbadad_COMMITS"
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Verifying rows saved to Vertica is within user tolerance...
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Number of rows_rejected=0. rows_copied=20. failedRowsPercent=0.0. user's failed_rows_percent_tolerance=0.0. passedFaultToleranceTest=true...PASSED.  OK to commit to database.
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Dropping Vertica rejects table now: DROP TABLE IF EXISTS "dftest_bb2e6fe9_c72a_4c10_af81_7d7a00fbadad_COMMITS" CASCADE
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Committing data into Vertica.
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Timed operation: Copy and commit data into Vertica -- took 326 ms.
22/12/13 17:45:35 INFO OverwriteByExpressionExec: Data source write support com.vertica.spark.datasource.v2.VerticaBatchWrite@57fe6f2d committed.
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:35 INFO VerticaJdbcLayer: Connecting to Vertica with URI: jdbc:vertica://vertica:5433/docker
22/12/13 17:45:35 INFO VerticaJdbcLayer: main: Successfully connected to Vertica.
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:35 INFO VerticaScanBuilder: Vertica 12.0.1-0 does not support writing the following complex types columns: . Export will be written to JSON instead.
22/12/13 17:45:35 INFO VerticaScanBuilder: Vertica 12.0.1-0 does not support writing the following complex types columns: . Export will be written to JSON instead.
22/12/13 17:45:35 INFO V2ScanRelationPushDown: 
Output: col1#4L
         
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:35 INFO VerticaJdbcLayer: Kerberos is not enabled in the hadoop config.
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSAuth
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSRegion
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSSessionToken
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSEndpoint
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSEnableHttps
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set S3EnableVirtualAddressing
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not setup GCS authentications
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Creating unique directory: webhdfs://hdfs:50070/data/d4791632_3c9a_45bd_87ff_14f8841c1ea2 with permissions: 700
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Select clause requested: "col1"
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Pushdown filters: 
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Export Source: "dftest"
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Exporting using statement: 
EXPORT TO PARQUET(directory = 'webhdfs://hdfs:50070/data/d4791632_3c9a_45bd_87ff_14f8841c1ea2/dftest', fileSizeMB = 4096, rowGroupSizeMB = 16, fileMode = '700', dirMode = '700') AS SELECT "col1" FROM "dftest";
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Timed operation: Export To Parquet From Vertica -- took 122 ms.
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Requested partition count: 1
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Parquet file list size: 1
22/12/13 17:45:35 INFO BlockManagerInfo: Removed broadcast_0_piece0 on fcd239af6c6b:41747 in memory (size: 10.2 KiB, free: 1048.8 MiB)
22/12/13 17:45:35 INFO BlockManagerInfo: Removed broadcast_0_piece0 on 172.19.0.2:39883 in memory (size: 10.2 KiB, free: 434.4 MiB)
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Total row groups: 1
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Creating partitions.
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Timed operation: Reading Parquet Files Metadata and creating partitions -- took 343 ms.
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Reading data from Parquet file.
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS credentials provider for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS auth for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not set AWS session token for Hadoop config
22/12/13 17:45:35 INFO HadoopFileStoreLayer: Did not load Google Cloud Storage service account authentications
22/12/13 17:45:35 INFO VerticaJdbcLayer: Connecting to Vertica with URI: jdbc:vertica://vertica:5433/docker
22/12/13 17:45:35 INFO VerticaJdbcLayer: main: Successfully connected to Vertica.
22/12/13 17:45:35 INFO VerticaJdbcLayer: Kerberos is not enabled in the hadoop config.
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSAuth
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSRegion
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSSessionToken
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSEndpoint
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set AWSEnableHttps
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not set S3EnableVirtualAddressing
22/12/13 17:45:35 INFO VerticaJdbcLayer: Did not setup GCS authentications
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Creating unique directory: webhdfs://hdfs:50070/data/d4791632_3c9a_45bd_87ff_14f8841c1ea2 with permissions: 700
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Directory already existed: webhdfs://hdfs:50070/data/d4791632_3c9a_45bd_87ff_14f8841c1ea2
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Select clause requested: "col1"
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Pushdown filters: 
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Export Source: "dftest"
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Export already done, skipping export step.
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Requested partition count: 1
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Parquet file list size: 1
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Total row groups: 1
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Creating partitions.
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Timed operation: Reading Parquet Files Metadata and creating partitions -- took 26 ms.
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Reading data from Parquet file.
22/12/13 17:45:36 INFO CodeGenerator: Code generated in 14.124034 ms
22/12/13 17:45:36 INFO SparkContext: Starting job: show at BasicReadWriteExamples.scala:88
22/12/13 17:45:36 INFO DAGScheduler: Got job 1 (show at BasicReadWriteExamples.scala:88) with 1 output partitions
22/12/13 17:45:36 INFO DAGScheduler: Final stage: ResultStage 1 (show at BasicReadWriteExamples.scala:88)
22/12/13 17:45:36 INFO DAGScheduler: Parents of final stage: List()
22/12/13 17:45:36 INFO DAGScheduler: Missing parents: List()
22/12/13 17:45:36 INFO DAGScheduler: Submitting ResultStage 1 (MapPartitionsRDD[7] at show at BasicReadWriteExamples.scala:88), which has no missing parents
22/12/13 17:45:36 INFO MemoryStore: Block broadcast_1 stored as values in memory (estimated size 11.3 KiB, free 1048.8 MiB)
22/12/13 17:45:36 INFO MemoryStore: Block broadcast_1_piece0 stored as bytes in memory (estimated size 5.7 KiB, free 1048.8 MiB)
22/12/13 17:45:36 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on fcd239af6c6b:41747 (size: 5.7 KiB, free: 1048.8 MiB)
22/12/13 17:45:36 INFO SparkContext: Created broadcast 1 from broadcast at DAGScheduler.scala:1513
22/12/13 17:45:36 INFO DAGScheduler: Submitting 1 missing tasks from ResultStage 1 (MapPartitionsRDD[7] at show at BasicReadWriteExamples.scala:88) (first 15 tasks are for partitions Vector(0))
22/12/13 17:45:36 INFO TaskSchedulerImpl: Adding task set 1.0 with 1 tasks resource profile 0
22/12/13 17:45:36 INFO TaskSetManager: Starting task 0.0 in stage 1.0 (TID 1) (172.19.0.2, executor 0, partition 0, PROCESS_LOCAL, 5073 bytes) taskResourceAssignments Map()
22/12/13 17:45:36 INFO BlockManagerInfo: Added broadcast_1_piece0 in memory on 172.19.0.2:39883 (size: 5.7 KiB, free: 434.4 MiB)
22/12/13 17:45:36 INFO TaskSetManager: Finished task 0.0 in stage 1.0 (TID 1) in 371 ms on 172.19.0.2 (executor 0) (1/1)
22/12/13 17:45:36 INFO TaskSchedulerImpl: Removed TaskSet 1.0, whose tasks have all completed, from pool 
22/12/13 17:45:36 INFO DAGScheduler: ResultStage 1 (show at BasicReadWriteExamples.scala:88) finished in 0.386 s
22/12/13 17:45:36 INFO DAGScheduler: Job 1 is finished. Cancelling potential speculative or zombie tasks for this job
22/12/13 17:45:36 INFO TaskSchedulerImpl: Killing all running tasks in stage 1: Stage finished
22/12/13 17:45:36 INFO DAGScheduler: Job 1 finished: show at BasicReadWriteExamples.scala:88, took 0.391499 s
22/12/13 17:45:36 INFO CodeGenerator: Code generated in 11.792328 ms
+----+
|col1|
+----+
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
|  77|
+----+

22/12/13 17:45:36 INFO ApplicationParquetCleaner: Removed webhdfs://hdfs:50070/data/d4791632_3c9a_45bd_87ff_14f8841c1ea2
22/12/13 17:45:36 INFO SparkUI: Stopped Spark web UI at http://fcd239af6c6b:4040
22/12/13 17:45:36 INFO StandaloneSchedulerBackend: Shutting down all executors
22/12/13 17:45:36 INFO CoarseGrainedSchedulerBackend$DriverEndpoint: Asking each executor to shut down
22/12/13 17:45:36 INFO MapOutputTrackerMasterEndpoint: MapOutputTrackerMasterEndpoint stopped!
22/12/13 17:45:36 INFO MemoryStore: MemoryStore cleared
22/12/13 17:45:36 INFO BlockManager: BlockManager stopped
22/12/13 17:45:36 INFO BlockManagerMaster: BlockManagerMaster stopped
22/12/13 17:45:36 INFO OutputCommitCoordinator$OutputCommitCoordinatorEndpoint: OutputCommitCoordinator stopped!
22/12/13 17:45:36 INFO SparkContext: Successfully stopped SparkContext
------------------------------------
-
- EXAMPLE: Data written to Vertica 
-
------------------------------------
22/12/13 17:45:36 INFO SparkContext: SparkContext already stopped.
22/12/13 17:45:36 INFO ShutdownHookManager: Shutdown hook called
22/12/13 17:45:36 INFO ShutdownHookManager: Deleting directory /tmp/spark-2c2ac0eb-938d-48ed-8d44-b3567164fb39
22/12/13 17:45:36 INFO ShutdownHookManager: Deleting directory /tmp/spark-17ebde17-89bf-447f-a30a-be750c8b9d52
```

</details>

### Vertica Logs

To find the Vertica logs it's best to follow the [Vertica documentation](https://www.vertica.com/docs/12.0.x/HTML/Content/Authoring/AdministratorsGuide/Monitoring/Vertica/MonitoringLogFiles.htm) in order to correctly identify the log files for monitoring.

If you are running the Spark-Connector's Vertica Docker container, the log files can be found at
```
/home/dbadmin/docker/v_docker_node0001_catalog/vertica.log
```

</details>

## Timed Operations

We have the option of passing a parameter to our write job that times certain operations. For instance, if we look at the Spark-Connector examples in ```/spark-connector/examples/scala/src/main/scala/example/examples/BasicReadWriteExamples.scala``` we have a basic job that writes to Vertica then reads it.

This job falls under the writeThenRead function, and contains the following code to start the write:

```    
  df.write.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName))
        .mode(mode)
        .save()
```

If we add ```timed_operations``` as a parameter along with the string "true," this will tell the connector it needs to time some operations.

```
      df.write.format(VERTICA_SOURCE)
        .options(options + ("table" -> tableName, "time_operations" -> "true"))
        .mode(mode)
        .save()
```

Some things to look for are:

```
22/12/13 17:45:35 INFO VerticaDistributedFilesystemWritePipe: Timed operation: Copy and commit data into Vertica -- took 326 ms.
```

Where the Connector is writing data into Vertica through the pipe.

```
22/12/13 17:45:35 INFO VerticaDistributedFilesystemReadPipe: Timed operation: Export To Parquet From Vertica -- took 122 ms.
```

Where the Connector is exporting data from Vertica into our intermediary storage in Parquet format.

```
22/12/13 17:45:36 INFO VerticaDistributedFilesystemReadPipe: Timed operation: Reading Parquet Files Metadata and creating partitions -- took 26 ms.
```

The operation that reads the metadata on the intermediary Parquet files and prepares for Spark's extraction of the data.