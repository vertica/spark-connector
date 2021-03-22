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

import com.vertica.spark.config.{DistributedFilesystemWriteConfig, EscapeUtils, TableName, VerticaMetadata, VerticaWriteMetadata}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.{JdbcLayerInterface, JdbcUtils}
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{CommitError, CreateTableError, DropTableError, DuplicateColumnsError, FaultToleranceTestFail, SchemaColumnListError, TempTableExistsError, ViewExistsError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface
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

  private val logger = config.logProvider.getLogger(classOf[VerticaDistributedFilesystemWritePipe])

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
   * Initial setup for the intermediate-based write operation.
   *
   * - Checks if the table exists
   * - If not, creates the table (based on user supplied statement or the one we build)
   * - Creates the directory that files will be exported to
   */
  def doPreWriteSteps(): ConnectorResult[Unit] = {
    for {
      // Check if schema is valid
      _ <- checkSchemaForDuplicates(config.schema)

      // If overwrite mode, remove table and force creation of new one before writing
      _ <- if(config.isOverwrite) tableUtils.dropTable(config.tablename) else Right(())

      // Create the table if it doesn't exist
      tableExistsPre <- tableUtils.tableExists(config.tablename)

      // Overwrite safety check
      _ <- if (config.isOverwrite && tableExistsPre) Left(DropTableError(None)) else Right(())

      // Check if a view exists or temp table exits by this name
      viewExists <- tableUtils.viewExists(config.tablename)
      _ <- if (viewExists) Left(ViewExistsError()) else Right(())
      tempTableExists <- tableUtils.tempTableExists(config.tablename)
      _ <- if (tempTableExists) Left(TempTableExistsError()) else Right(())

      _ <- if (!tableExistsPre) tableUtils.createTable(config.tablename, config.targetTableSql, config.schema, config.strlen) else Right(())

      // Confirm table was created. This should only be false if the user specified an invalid target_table_sql
      tableExistsPost <- tableUtils.tableExists(config.tablename)
      _ <- if (tableExistsPost) Right(()) else Left(CreateTableError(None))

      // Create the directory to export files to
      _ <- fileStoreLayer.createDir(config.fileStoreConfig.address)

      // Create job status table / entry
      _ <- tableUtils.createAndInitJobStatusTable(config.tablename, config.jdbcConfig.username, config.sessionId, if(config.isOverwrite) "OVERWRITE" else "APPEND")
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
    jdbcLayer.close()
    fileStoreLayer.closeWriteParquetFile()
  }


  def buildCopyStatement(targetTable: String, columnList: String, url: String, rejectsTableName: String, fileFormat: String): String = {
    s"COPY $targetTable $columnList FROM '$url' ON ANY NODE $fileFormat REJECTED DATA AS TABLE $rejectsTableName NO COMMIT"
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
        schemaTools.getCopyColumnList(jdbcLayer, config.tablename.getFullTableName, config.schema)
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

  def commit(): ConnectorResult[Unit] = {
    val globPattern: String = "*.parquet"

    // Create url string, escape any ' characters as those surround the url
    val url: String = EscapeUtils.sqlEscape(s"${config.fileStoreConfig.address.stripSuffix("/")}/$globPattern")

    val tableNameMaxLength = 30

    val ret = for {
      // Get columnList
      columnList <- getColumnList.left.map(_.context("commit: Failed to get column list"))

      tableName = config.tablename.name
      sessionId = config.sessionId
      rejectsTableName = "\"" +
        EscapeUtils.sqlEscape(tableName.substring(0,Math.min(tableNameMaxLength,tableName.length))) +
        "_" +
        sessionId +
        "_COMMITS" +
        "\""

      copyStatement = buildCopyStatement(config.tablename.getFullTableName,
        columnList,
        url,
        rejectsTableName,
        "parquet"
      )

      rowsCopied <- performCopy(copyStatement, config.tablename).left.map(_.context("commit: Failed to copy rows"))

      faultToleranceResults <- testFaultTolerance(rowsCopied, rejectsTableName)
        .left.map(err => CommitError(err).context("commit: JDBC Error when trying to determine fault tolerance"))

      _ <- tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.username, faultToleranceResults.failedRowsPercent, config.sessionId, faultToleranceResults.success)

      _ <- if (faultToleranceResults.success) Right(()) else Left(FaultToleranceTestFail())
    } yield ()

    // Commit or rollback
    val result: ConnectorResult[Unit] = ret match {
      case Right(_) =>
        jdbcLayer.commit().left.map(err => CommitError(err).context("JDBC Error when trying to commit"))
      case Left(retError) =>
        jdbcLayer.rollback() match {
          case Right(_) => Left(retError)
          case Left(err) => Left(retError.context("JDBC Error when trying to rollback: " + err.getFullContext))
        }
    }

    fileStoreLayer.removeDir(config.fileStoreConfig.address)
    jdbcLayer.close()
    result
  }
}
