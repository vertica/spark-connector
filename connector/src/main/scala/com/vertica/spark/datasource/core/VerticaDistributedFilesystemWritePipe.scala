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
import com.vertica.spark.util.error.CreateExternalTableMergeKey
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.CreateExternalTableAlreadyExistsError
import com.vertica.spark.util.error._
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.internal.SQLConf
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
        session.sparkContext.setLocalProperty(SQLConf.LEGACY_PARQUET_REBASE_MODE_IN_WRITE.key , "CORRECTED")
        // don't use SQLConf because that breaks things for users on Spark 3.0
        session.sparkContext.setLocalProperty("spark.sql.legacy.parquet.int96RebaseModeInWrite"
          , "CORRECTED")
      case None => logger.warn("No spark session found to set config")
    }
  }

  /**
   * Initial setup for the intermediate-based write operation.
   *
   * - Checks if the table exists
   * - If not, creates the table (based on user supplied statement or the one we build)
   * - Creates the directory that files will be exported to
   */
  def doPreWriteSteps(): ConnectorResult[Unit] = {
    if(config.mergeKey.isDefined && config.isOverwrite) logger.warn("Save mode is specified as Overwrite during a merge.")
    for {
      // Check if schema is valid
      _ <- checkSchemaForDuplicates(config.schema)

      // Set spark configuration
      _ = setSparkCalendarConf()

      // If overwrite mode, remove table and force creation of new one before writing
      _ <- if(config.isOverwrite && config.mergeKey.isEmpty) tableUtils.dropTable(config.tablename) else Right(())

      // Creating external table and merging at the same time not supported
      _ <- if (config.createExternalTable && config.mergeKey.isDefined) Left(CreateExternalTableMergeKey()) else Right(())

      // Create the table if it doesn't exist
      tableExistsPre <- tableUtils.tableExists(config.tablename)

      // Overwrite safety check
      _ <- if (config.isOverwrite && config.mergeKey.isEmpty && tableExistsPre) Left(DropTableError()) else Right(())

      // External table mode doesn't work if table exists
      _ <- if (config.createExternalTable && tableExistsPre) Left(CreateExternalTableAlreadyExistsError()) else Right()

      // Check if a view exists or temp table exists by this name
      viewExists <- tableUtils.viewExists(config.tablename)
      _ <- if (viewExists) Left(ViewExistsError()) else Right(())
      tempTableExists <- tableUtils.tempTableExists(config.tablename)
      _ <- if (tempTableExists) Left(TempTableExistsError()) else Right(())

      // Create table unless we're appending, or we're in external table mode (that gets created later)
      _ <- if (!tableExistsPre && !config.createExternalTable) tableUtils.createTable(config.tablename, config.targetTableSql, config.schema, config.strlen) else Right(())

      // Confirm table was created. This should only be false if the user specified an invalid target_table_sql
      tableExistsPost <- tableUtils.tableExists(config.tablename)
      _ <- if (tableExistsPost || config.createExternalTable) Right(()) else Left(CreateTableError(None))

      // Create the directory to export files to
      perm = config.filePermissions
      _ <- fileStoreLayer.createDir(config.fileStoreConfig.address, perm.toString)

      // Create job status table / entry
      _ <- tableUtils.createAndInitJobStatusTable(config.tablename, config.jdbcConfig.auth.user, config.sessionId, if(config.isOverwrite) "OVERWRITE" else "APPEND")
    } yield ()
  }

  def startPartitionWrite(uniqueId: String): ConnectorResult[Unit] = {
    val address = config.fileStoreConfig.address
    val delimiter = if(address.takeRight(1) == "/" || address.takeRight(1) == "\\") "" else "/"
    val filename = address + delimiter + uniqueId + ".parquet"

    fileStoreLayer.openWriteParquetFile(filename)
  }

  def writeData(data: DataBlock): ConnectorResult[Unit] = {
    fileStoreLayer.writeDataToParquetFile(data)
  }

  def endPartitionWrite(): ConnectorResult[Unit] = {
    fileStoreLayer.closeWriteParquetFile()
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
      case Some(key) => key.toString.split(",").toList.map(col => s"target.$col=temp.$col").mkString(" AND ")
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
    ret.left.map(err => CommitError(err).context("performMerge: JDBC error when trying to merge"))

  }

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
        Right("(" + list + ")")
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

    ret.left.map(err => CommitError(err).context("performCopy: JDBC error when trying to copy"))
  }

  def commitDataIntoVertica(url: String): ConnectorResult[Unit] = {
    val tableNameMaxLength = 30

    val ret = for {
      // Set Vertica to work with kerberos and HDFS/AWS
      _ <- jdbcLayer.configureSession(fileStoreLayer)

      // Get columnList
      columnList <- getColumnList.left.map(_.context("commit: Failed to get column list"))

      // Do not check for mergeKey here, as tableName does not impact merge
      tableName = config.tablename.name
      sessionId = config.sessionId

      _ <- if (config.mergeKey.isDefined) tableUtils.createTempTable(tempTableName, config.schema, config.strlen) else Right(())

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

      rowsCopied <- if (config.mergeKey.isDefined) {
                      Right(performCopy(copyStatement, tempTableName).left.map(_.context("commit: Failed to copy rows into temp table")))
                    }
                    else {
                      Right(performCopy(copyStatement, config.tablename).left.map(_.context("commit: Failed to copy rows into target table")))
                    }

      faultToleranceResults <- testFaultTolerance(rowsCopied.right.getOrElse(0), rejectsTableName)
        .left.map(err => CommitError(err).context("commit: JDBC Error when trying to determine fault tolerance"))

      _ <- tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.auth.user, faultToleranceResults.failedRowsPercent, config.sessionId, faultToleranceResults.success)

      _ <- if (faultToleranceResults.success) Right(()) else Left(FaultToleranceTestFail())

      mergeStatement <- if (config.mergeKey.isDefined) {
                          Right(buildMergeStatement(config.tablename, columnList, tempTableName.getFullTableName))
                        }
                        else {
                          Right("")
                        }
      _ <- if (config.mergeKey.isDefined) performMerge(mergeStatement) else Right(())

    } yield ()

    fileStoreLayer.removeDir(config.fileStoreConfig.address)
    ret
  }

  def commitDataAsExternalTable(url: String): ConnectorResult[Unit] = {
    if(config.copyColumnList.isDefined) {
      logger.warn("Custom copy column list was specified, but will be ignored when creating new external table.")
    }

    val ret = for {
      _ <- tableUtils.createExternalTable(
        tablename = config.tablename,
        targetTableSql = config.targetTableSql,
        schema = config.schema,
        strlen = config.strlen,
        urlToCopyFrom = url
      )

      _ <- tableUtils.validateExternalTable(config.tablename)

      _ <- tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.auth.user, 0.0, config.sessionId, true)

    } yield ()

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
    val url: String = EscapeUtils.sqlEscape(s"${config.fileStoreConfig.address.stripSuffix("/")}/$globPattern")

    val ret = if(config.createExternalTable) {
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
