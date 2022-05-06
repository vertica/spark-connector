// (c) Copyright [2020-2021] Micro Focus or one of its affiliates.
// Licensed under the Apache License, Version 2.0 (the "License");
// You may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.vertica.spark.datasource.core

import com.vertica.spark.config._
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcUtils}
import com.vertica.spark.util.Timer
import com.vertica.spark.util.error.CreateExternalTableMergeKey
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.CreateExternalTableAlreadyExistsError
import com.vertica.spark.util.error._
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
import com.vertica.spark.util.version.VerticaVersionUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StructType

import scala.util.Try

/**
 * Pipe for writing data to Vertica using an intermediary filesystem.
 *
 * @param config Configuration data for writing to Vertica
 * @param fileStoreLayer Dependency for communication with the intermediary filesystem
 * @param jdbcLayer Dependency for communication with the database over JDBC
 * @param schemaTools Dependency for schema conversion between Vertica and Spark
 * @param tableUtils Depedency on top of JDBC layer for interacting with tables
 * @param dataSize Number of rows per data block
 */
class VerticaDistributedFilesystemWritePipe(val config: DistributedFilesystemWriteConfig,
                                            val fileStoreLayer: FileStoreLayerInterface,
                                            val jdbcLayer: JdbcLayerInterface,
                                            val schemaTools: SchemaToolsInterface,
                                            val tableUtils: TableUtilsInterface,
                                            val dataSize: Int = 1)
          extends VerticaPipeInterface with VerticaPipeWriteInterface {

  private val logger = LogProvider.getLogger(classOf[VerticaDistributedFilesystemWritePipe])
  private val tempTableName = TableName(config.tablename.name + "_" + config.sessionId, None)
  private val LEGACY_PARQUET_REBASE_MODE_IN_WRITE = "spark.sql.legacy.parquet.datetimeRebaseModeInWrite"
  private val LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE = "spark.sql.legacy.parquet.int96RebaseModeInWrite"

  /**
   * No write metadata required for configuration as of yet.
   */
  def getMetadata: ConnectorResult[VerticaMetadata] = Right(VerticaWriteMetadata())

  /**
   * Returns the number of rows used per data block.
   */
  def getDataBlockSize: ConnectorResult[Long] = Right(dataSize)


  def checkSchemaForDuplicates(schema: StructType): ConnectorResult[Unit] = {
    val names = schema.fields.map(f => f.name)
    if(names.distinct.length != names.length) {
      Left(DuplicateColumnsError())
    } else {
      Right(())
    }
  }

  /**
   * Set spark conf to handle old dates if unset
   * This deals with SPARK-31404 -- issue with legacy calendar format
   */
  private def setSparkCalendarConf(): Unit = {
    SparkSession.getActiveSession match {
      case Some(session) =>
        session.sparkContext.setLocalProperty(LEGACY_PARQUET_REBASE_MODE_IN_WRITE, "CORRECTED")
        session.sparkContext.setLocalProperty(LEGACY_PARQUET_INT96_REBASE_MODE_IN_WRITE, "CORRECTED")
      case None => logger.warn("No spark session found to set config")
    }
  }

  /**
   * Determine the address where the data will be written.  This varies depending on if it is a new external table or not.
   */
  private def getAddress(): String = {
    if (config.createExternalTable.isDefined) config.fileStoreConfig.externalTableAddress else config.fileStoreConfig.address
  }

  /**
   * Initial setup for the intermediate-based write operation.
   *
   * - Checks if the table exists
   * - If not, creates the table (based on user supplied statement or the one we build)
   * - Creates the directory that files will be exported to
   */
  def doPreWriteSteps(): ConnectorResult[Unit] = {
    // Log a warning if the user wants to perform a merge, but also wants to overwrite data
    if(config.mergeKey.isDefined && config.isOverwrite) logger.warn("Save mode is specified as Overwrite during a merge.")
    logger.info("Writing data to Parquet file.")
    for {
      // Check if schema is valid
      _ <- checkSchemaForDuplicates(config.schema)

      // Ensure Vertica version is compatible.
      _ <- checkSchemaTypesSupport(config)

      // Set spark configuration
      _ = setSparkCalendarConf()

      // If overwrite mode, remove table and force creation of new one before writing
      _ <- if (config.isOverwrite && config.mergeKey.isEmpty) tableUtils.dropTable(config.tablename) else Right(())

      // Creating external table and merging at the same time not supported
      _ <- if (config.createExternalTable.isDefined && config.mergeKey.isDefined) Left(CreateExternalTableMergeKey()) else Right(())

      // Create the table if it doesn't exist
      tableExistsPre <- tableUtils.tableExists(config.tablename)

      // Overwrite safety check
      _ <- if (config.isOverwrite && config.mergeKey.isEmpty && tableExistsPre) Left(DropTableError()) else Right(())

      // External table mode doesn't work if table exists
      _ <- if (config.createExternalTable.isDefined && tableExistsPre) Left(CreateExternalTableAlreadyExistsError()) else Right()

      // Check if a view exists or temp table exists by this name
      viewExists <- tableUtils.viewExists(config.tablename)
      _ <- if (viewExists) Left(ViewExistsError()) else Right(())
      tempTableExists <- tableUtils.tempTableExists(config.tablename)
      _ <- if (tempTableExists) Left(TempTableExistsError()) else Right(())

      // Create table unless we're appending, or we're in external table mode (that gets created later)
      _ <- if (!tableExistsPre && config.createExternalTable.isEmpty) tableUtils.createTable(config.tablename, config.targetTableSql, config.schema, config.strlen, config.arrayLength) else Right(())

      // Confirm table was created. This should only be false if the user specified an invalid target_table_sql
      tableExistsPost <- tableUtils.tableExists(config.tablename)
      _ <- if (tableExistsPost || config.createExternalTable.isDefined) Right(()) else Left(CreateTableError(None))

      // Create the directory to export files to
      perm = config.filePermissions
      existingData = config.createExternalTable match {
        case Some(value) =>
          value match {
            case ExistingData => true
            case NewData => false
          }
        case None => false
      }
      _ <- if(existingData) Right(()) else fileStoreLayer.createDir(getAddress(), perm.toString)

      // Create job status table / entry
      _ <- if(config.saveJobStatusTable) {
        tableUtils.createAndInitJobStatusTable(config.tablename, config.jdbcConfig.auth.user, config.sessionId, if(config.isOverwrite) "OVERWRITE" else "APPEND")
      } else {
        Right(())
      }
    } yield ()
  }

  private def checkSchemaTypesSupport(config: DistributedFilesystemWriteConfig): ConnectorResult[Unit] = {
    val verticaVersion = VerticaVersionUtils.getVersion(jdbcLayer)
    val toInternalTable = config.createExternalTable.isEmpty
    VerticaVersionUtils.checkSchemaTypesWriteSupport(config.schema, verticaVersion, toInternalTable)
  }

  val timer = new Timer(config.timeOperations, logger, "Writing Partition.")

  def startPartitionWrite(uniqueId: String): ConnectorResult[Unit] = {
    val address = getAddress()
    val delimiter = if(address.takeRight(1) == "/" || address.takeRight(1) == "\\") "" else "/"
    val filename = address + delimiter + uniqueId + ".snappy.parquet"

    timer.startTime()

    fileStoreLayer.openWriteParquetFile(filename) match {
      case Left(err) =>
        if(!config.fileStoreConfig.preventCleanup) {
          logger.info("Cleaning up all files in path: " + address)
          fileStoreLayer.removeDir(address)
        }
        Left(err)
      case Right(()) => Right()
    }
  }

  def writeData(data: DataBlock): ConnectorResult[Unit] = {
    config.createExternalTable match {
      case Some(value) =>
        value match {
          case ExistingData => Left(NonEmptyDataFrameError())
          case NewData => fileStoreLayer.writeDataToParquetFile(data)
        }

      case None => fileStoreLayer.writeDataToParquetFile(data)
    }
  }

  def endPartitionWrite(): ConnectorResult[Unit] = {
    timer.endTime()
    config.createExternalTable match {
      case Some(value) =>
        value match {
          case ExistingData => Right(())
          case NewData => fileStoreLayer.closeWriteParquetFile()
        }
      case None =>  fileStoreLayer.closeWriteParquetFile()
    }
  }

  def buildCopyStatement(targetTable: String, columnList: String, url: String, rejectsTableName: String, fileFormat: String): String = {
    if (config.mergeKey.isDefined) {
      s"COPY $targetTable FROM '$url' ON ANY NODE $fileFormat REJECTED DATA AS TABLE $rejectsTableName NO COMMIT"
    }
    else {
      s"COPY $targetTable $columnList FROM '$url' ON ANY NODE $fileFormat REJECTED DATA AS TABLE $rejectsTableName NO COMMIT"
    }
  }

  def buildMergeStatement(targetTableName: TableName, columnList: String, tempTable: String): String = {
    val targetTable = targetTableName.getFullTableName
    val updateColValues = schemaTools.getMergeUpdateValues(jdbcLayer, targetTableName, tempTableName, config.copyColumnList) match {
      case Right(values) => values
      case Left(err) => Left(MergeColumnListError(err))
    }
    val insertColValues = schemaTools.getMergeInsertValues(jdbcLayer, tempTableName, config.copyColumnList) match {
      case Right(values) => values
      case Left(err) => Left(MergeColumnListError(err))
    }
    val mergeList = config.mergeKey match {
      case Some(key) =>
        val trimmedCols = key.toString.split(",").toList.map(col => col.trim())
        trimmedCols.map(trimmedCol => s"target.$trimmedCol=temp.$trimmedCol").mkString(" AND ")

      case None => List()
    }
    s"MERGE INTO $targetTable as target using $tempTable as temp ON ($mergeList) WHEN MATCHED THEN UPDATE SET $updateColValues WHEN NOT MATCHED THEN INSERT $columnList VALUES ($insertColValues)"
  }

  def performMerge (mergeStatement: String): ConnectorResult[Unit] = {
    val ret = for {

      // Explain merge first to verify it's valid.
      rs <- jdbcLayer.query("EXPLAIN " + mergeStatement)
      _ = rs.close()

      // Real merge
      _ <- jdbcLayer.execute(mergeStatement)
    } yield ()
    logger.info("Executing merge")
    ret.left.map(err => CommitError(err).context( "performMerge: JDBC error when trying to merge"))

  }

  def inferExternalTableSchema(): ConnectorResult[String] = {
    val tableName = config.tablename.getFullTableName.replaceAll("\"","")

    val inferStatement =
      fileStoreLayer.getGlobStatus(EscapeUtils.sqlEscape(s"${config.fileStoreConfig.externalTableAddress.stripSuffix("/")}/*.parquet")) match {
        case Right(list) =>
          val url: String = {
            if (list.nonEmpty) {
              EscapeUtils.sqlEscape(s"${config.fileStoreConfig.externalTableAddress.stripSuffix("/")}/*.parquet")
            }
            else {
              EscapeUtils.sqlEscape(s"${config.fileStoreConfig.externalTableAddress.stripSuffix("/")}/**/*.parquet")
            }
          }
          buildInferStatement(url, tableName)

        case Left(err) => err.getFullContext
      }

    logger.debug("The infer statement is: " + inferStatement)
    jdbcLayer.query(inferStatement) match {
      case Left(err) => Left(InferExternalTableSchemaError(err))
      case Right(resultSet) =>
        try {
          resultSet.next
          val createExternalTableStatement = resultSet.getString(1)
          val isPartitioned = inferStatement.contains(EscapeUtils.sqlEscape(s"${config.fileStoreConfig.externalTableAddress.stripSuffix("/")}/**/*.parquet"))

          if(!isPartitioned && !createExternalTableStatement.contains("varchar") && !createExternalTableStatement.contains("varbinary")) {
            logger.info("Inferring schema from parquet data")
            val updatedStatement = createExternalTableStatement.replace("\"" + tableName + "\"", tableName)
            logger.debug("The create external table statement is: " + updatedStatement)
            Right(updatedStatement)
          }
          else {
            logger.info("Inferring partial schema from dataframe")
            schemaTools.inferExternalTableSchema(createExternalTableStatement, config.schema, tableName, config.strlen, config.arrayLength)
          }
        }
        catch {
          case e: Throwable =>
            Left(InferExternalSchemaError(e))
        }
        finally {
          resultSet.close()
        }
    }
  }

  protected def buildInferStatement(url: String, tableName: String): String =
    s"SELECT INFER_TABLE_DDL('$url' USING PARAMETERS format = 'parquet', table_name = '$tableName', table_type = 'external');"

  /**
   * Function to get column list to use for the operation
   *
   * Two options depending on configuration and specified table:
   * - Custom column list provided by the user
   * - Column list built for a subset of rows in the table that match our schema
   */
  private def getColumnList: ConnectorResult[String] = {
    config.copyColumnList match {
      case Some(list) =>
        logger.info(s"Using custom COPY column list. " + "Target table: " + config.tablename.getFullTableName +
          ", " + "copy_column_list: " + list + ".")
        Right("(" + list.toString.split(",").map(col => col.trim()).mkString(",") + ")")
      case None =>
        // Default COPY
        logger.info(s"Building default copy column list")
        schemaTools.getCopyColumnList(jdbcLayer, config.tablename, config.schema)
          .left.map(err => SchemaColumnListError(err)
          .context("getColumnList: Error building default copy column list"))
    }
  }

  private case class FaultToleranceTestResult(success: Boolean, failedRowsPercent: Double)

  private def testFaultTolerance(rowsCopied: Int, rejectsTable: String) : ConnectorResult[FaultToleranceTestResult] = {
    // verify rejects to see if this falls within user tolerance.
    val rejectsQuery = "SELECT COUNT(*) as count FROM " + rejectsTable
    logger.info(s"Checking number of rejected rows via statement: " + rejectsQuery)

    for {
      rs <- jdbcLayer.query(rejectsQuery)
      res = Try {
        if (rs.next) {
          rs.getInt("count")
        }
        else {
          logger.error("Could not retrieve rejected row count.")
          0
        }
      }
      _ = rs.close()
      rejectedCount <- JdbcUtils.tryJdbcToResult(jdbcLayer, res)

      failedRowsPercent = if (rowsCopied > 0) {
        rejectedCount.toDouble / (rowsCopied.toDouble + rejectedCount.toDouble)
      } else {
        1.0
      }


      passedFaultToleranceTest = failedRowsPercent <= config.failedRowPercentTolerance.toDouble

      // Report save status message to user either way.
      tolerance_message =
        "Number of rows_rejected=" + rejectedCount +
          ". rows_copied=" + rowsCopied +
          ". failedRowsPercent=" + failedRowsPercent +
          ". user's failed_rows_percent_tolerance=" +
          config.failedRowPercentTolerance +
          ". passedFaultToleranceTest=" + passedFaultToleranceTest.toString

      _ = logger.info(s"Verifying rows saved to Vertica is within user tolerance...")
      _ = logger.info(tolerance_message + {
        if (passedFaultToleranceTest) {
          "...PASSED.  OK to commit to database."
        } else {
          "...FAILED.  NOT OK to commit to database"
        }
      })

      _ <- if (rejectedCount == 0) {
        val dropRejectsTableStatement = "DROP TABLE IF EXISTS " + rejectsTable  + " CASCADE"
        logger.info(s"Dropping Vertica rejects table now: " + dropRejectsTableStatement)
        jdbcLayer.execute(dropRejectsTableStatement)
      } else {
        // Log the first few rejected rows
        val rejectsDataQuery = "SELECT COUNT(*) count, MIN(rejected_data) example_data, rejected_reason FROM " + rejectsTable + " GROUP BY rejected_reason ORDER BY count DESC LIMIT 10"
        logger.info(s"Getting summary of rejected rows via statement: " + rejectsDataQuery)
        for {
          rs <- jdbcLayer.query(rejectsDataQuery)
          _ = Try {
            val rsmd = rs.getMetaData
            logger.error(s"Found $rejectedCount rejected rows, displaying up to 10 of the most common reasons:")
            logger.error((1 to rsmd.getColumnCount).map(idx => rsmd.getColumnName(idx)).toList.mkString(" | "))
            while (rs.next) {
              logger.error((1 to rsmd.getColumnCount).map(idx => rs.getString(idx)).toList.mkString(" | "))
            }
          }
          _ = rs.close()
        } yield ()

        Right(())
      }

      testResult = FaultToleranceTestResult(passedFaultToleranceTest, failedRowsPercent)
    } yield testResult
  }

  /**
   * Performs copy operations
   *
   * @return rows copied
   */
  def performCopy(copyStatement: String, tablename: TableName): ConnectorResult[Int] = {
    // Empty copy to make sure a projection is created if it hasn't been yet
    // This will error out, but create the projection
    val emptyCopy = "COPY " + tablename.getFullTableName + " FROM '';"
    jdbcLayer.executeUpdate(emptyCopy)

    val ret = for {

      // Explain copy first to verify it's valid.
      rs <- jdbcLayer.query("EXPLAIN " + copyStatement)
      _ = rs.close()

      // Real copy
      rowsCopied <- jdbcLayer.executeUpdate(copyStatement)
    } yield rowsCopied
    logger.info("Performing copy from file store to Vertica")
    ret.left.map(err => CommitError(err).context("performCopy: JDBC error when trying to copy"))
  }

  def commitDataIntoVertica(url: String): ConnectorResult[Unit] = {
    val tableNameMaxLength = 30
    val timer = new Timer(config.timeOperations, logger, "Copy and commit data into Vertica")
    timer.startTime()
    val ret = for {
      // Set Vertica to work with kerberos and HDFS/AWS
      _ <- jdbcLayer.configureSession(fileStoreLayer)

      // Get columnList
      columnList <- getColumnList.left.map(_.context("commit: Failed to get column list"))

      // Do not check for mergeKey here, as tableName does not impact merge
      tableName = config.tablename.name
      sessionId = config.sessionId

      _ <- if (config.mergeKey.isDefined) tableUtils.createTempTable(tempTableName, config.schema, config.strlen, config.arrayLength) else Right(())

      tempTableExists <- tableUtils.tempTableExists(tempTableName)
      _ <- if (config.mergeKey.isDefined && !tempTableExists) Left(CreateTableError(None)) else Right(())

      rejectsTableName = "\"" +
        EscapeUtils.sqlEscape(tableName.substring(0,Math.min(tableNameMaxLength,tableName.length))) +
        "_" +
        sessionId +
        "_COMMITS" +
        "\""

      fullTableName <- if(config.mergeKey.isDefined) Right(tempTableName.getFullTableName) else Right(config.tablename.getFullTableName)

      copyStatement = buildCopyStatement(fullTableName, columnList, url, rejectsTableName, "parquet")

      _ = logger.info("The copy statement is: \n" + copyStatement)

      rowsCopied <- if (config.mergeKey.isDefined) {
        performCopy(copyStatement, tempTableName).left.map(_.context("commit: Failed to copy rows into temp table"))
      }
      else {
        performCopy(copyStatement, config.tablename).left.map(_.context("commit: Failed to copy rows into target table"))
      }

      faultToleranceResults <- testFaultTolerance(rowsCopied, rejectsTableName)
        .left.map(err => CommitError(err).context("commit: JDBC Error when trying to determine fault tolerance"))

      _ <- if (config.saveJobStatusTable) {
        tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.auth.user, faultToleranceResults.failedRowsPercent, config.sessionId, faultToleranceResults.success)
      } else {
        Right(())
      }

      _ <- if (faultToleranceResults.success) Right(()) else Left(FaultToleranceTestFail())

      mergeStatement <- if (config.mergeKey.isDefined) {
                          Right(buildMergeStatement(config.tablename, columnList, tempTableName.getFullTableName))
                        }
                        else {
                          Right("")
                        }
      _ = if(config.mergeKey.isDefined) logger.info("The merge statement is: \n" + mergeStatement)
      _ <- if (config.mergeKey.isDefined) performMerge(mergeStatement) else Right(())

    } yield ()
    logger.info("Committing data into Vertica.")
    if(!config.fileStoreConfig.preventCleanup) fileStoreLayer.removeDir(config.fileStoreConfig.address)
    timer.endTime()
    ret
  }

  def commitDataAsExternalTable(url: String): ConnectorResult[Unit] = {
    val timer = new Timer(config.timeOperations, logger, "Commit data as external table")
    timer.startTime()
    logger.info("Committing data as external table.")
    if(config.copyColumnList.isDefined) {
      logger.warn("Custom copy column list was specified, but will be ignored when creating new external table.")
    }

    val ret = for {
      _ <- jdbcLayer.configureSession(fileStoreLayer)

      existingData = config.createExternalTable match {
        case Some(value) =>
          value match {
            case ExistingData => true
            case NewData => false
          }
        case None => false
      }
      createExternalTableStmt <- if(existingData) inferExternalTableSchema else Right(())

      _ <- tableUtils.createExternalTable(
                tablename = config.tablename,
                if(existingData) Some(createExternalTableStmt.toString) else config.targetTableSql,
                schema = config.schema,
                strlen = config.strlen,
                urlToCopyFrom = url,
                config.arrayLength
              )

      _ <- tableUtils.validateExternalTable(config.tablename, config.schema)

      _ <- if (config.saveJobStatusTable) {
        tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.auth.user, 0.0, config.sessionId, success = true)
      } else {
        Right(())
      }
    } yield ()

    timer.endTime()

    // External table creation always commits. So, if an error was detected, drop the table
    ret match {
      case Left(err) =>
        tableUtils.dropTable(config.tablename)
        Left(err)
      case Right(_) => Right()
    }
  }

  def commit(): ConnectorResult[Unit] = {
    val globPattern: String = "*.parquet"

    // Create url string, escape any ' characters as those surround the url
    val url: String = EscapeUtils.sqlEscape(s"${getAddress().stripSuffix("/")}/$globPattern")

    val ret = if(config.createExternalTable.isDefined) {
      commitDataAsExternalTable(url)
    }
    else {
      commitDataIntoVertica(url)
    }

    // Commit or rollback
    val result = ret match {
      case Right(_) =>
        jdbcLayer.commit().left.map(err => CommitError(err).context("JDBC Error when trying to commit"))
      case Left(retError) =>
        jdbcLayer.rollback() match {
          case Right(_) => Left(retError)
          case Left(err) => Left(retError.context("JDBC Error when trying to rollback: " + err.getFullContext))
        }
    }

    jdbcLayer.close()
    result
  }
}

/**
 * With changes made in Vertica 11.1, this class contains old implmentation to support previous Vertica versions.
 *
 * */
class VerticaDistributedFilesystemWritePipeOld(override val config: DistributedFilesystemWriteConfig,
                                               override val fileStoreLayer: FileStoreLayerInterface,
                                               override val jdbcLayer: JdbcLayerInterface,
                                               override val schemaTools: SchemaToolsInterface,
                                               override val tableUtils: TableUtilsInterface,
                                               override val dataSize: Int = 1)
  extends VerticaDistributedFilesystemWritePipe(config, fileStoreLayer, jdbcLayer, schemaTools, tableUtils, dataSize){

  override protected def buildInferStatement(url: String, tableName: String): String =
    "SELECT INFER_EXTERNAL_TABLE_DDL(" + "\'" + url + "\',\'" + tableName + "\')"
}
