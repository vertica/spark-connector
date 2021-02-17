package com.vertica.spark.datasource.core

import com.vertica.spark.config.{DistributedFilesystemWriteConfig, VerticaMetadata, VerticaWriteMetadata}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.{CommitError, CreateTableError, FaultToleranceTestFail, SchemaColumnListError, ViewExistsError}
import com.vertica.spark.util.error.{ConnectorError, JDBCLayerError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import com.vertica.spark.util.table.TableUtilsInterface

class VerticaDistributedFilesystemWritePipe(val config: DistributedFilesystemWriteConfig,
                                            val fileStoreLayer: FileStoreLayerInterface,
                                            val jdbcLayer: JdbcLayerInterface,
                                            val schemaTools: SchemaToolsInterface,
                                            val tableUtils: TableUtilsInterface,
                                            val dataSize: Int = 1)
          extends VerticaPipeInterface with VerticaPipeWriteInterface {

  private val logger = config.logProvider.getLogger(classOf[VerticaDistributedFilesystemWritePipe])

  // No write metadata required for configuration as of yet
  def getMetadata: Either[ConnectorError, VerticaMetadata] = Right(VerticaWriteMetadata())

  def getDataBlockSize: Either[ConnectorError, Long] = Right(dataSize)



  /**
   * Initial setup for the intermediate-based write operation.
   *
   * - Checks if the table exists
   * - If not, creates the table (based on user supplied statement or the one we build)
   * - Creates the directory that files will be exported to
   */
  def doPreWriteSteps(): Either[ConnectorError, Unit] = {
    // TODO: Write modes
    for {
      // Create the table if it doesn't exist
      tableExistsPre <- tableUtils.tableExists(config.tablename)
      viewExists <- tableUtils.viewExists(config.tablename)
      _ <- if(viewExists) Left(ConnectorError(ViewExistsError)) else Right(())
      _ <- if(!tableExistsPre) tableUtils.createTable(config.tablename, config.targetTableSql, config.schema, config.strlen) else Right(())

      // Confirm table was created. This should only be false if the user specified an invalid target_table_sql
      tableExistsPost <- tableUtils.tableExists(config.tablename)
      _ <- if(tableExistsPost) Right(()) else Left(ConnectorError(CreateTableError))

      // Create the directory to export files to
      _ <- fileStoreLayer.createDir(config.fileStoreConfig.address)

      // Create job status table / entry
      _ <- tableUtils.createAndInitJobStatusTable(config.tablename, config.jdbcConfig.username, config.sessionId)
    } yield ()
  }

  def startPartitionWrite(uniqueId: String): Either[ConnectorError, Unit] = {
    val address = config.fileStoreConfig.address
    val delimiter = if(address.takeRight(1) == "/" || address.takeRight(1) == "\\") "" else "/"
    val filename = address + delimiter + uniqueId + ".parquet"

    fileStoreLayer.openWriteParquetFile(filename)
  }

  def writeData(data: DataBlock): Either[ConnectorError, Unit] = {
    fileStoreLayer.writeDataToParquetFile(data)
  }

  def endPartitionWrite(): Either[ConnectorError, Unit] = {
    jdbcLayer.close()
    fileStoreLayer.closeWriteParquetFile()
  }


  def buildCopyStatement(targetTable: String, columnList: String, url: String, rejectsTableName: String, fileFormat: String): String = {
    s"COPY $targetTable $columnList FROM '$url' ON ANY NODE $fileFormat REJECTED DATA AS TABLE $rejectsTableName NO COMMIT"
  }

  /**
   * Function to get column list to use for the operation
   *
   * Three options depending on configuration and specified table:
   * - Custom column list provided by the user
   * - Column list built for a subset of rows in the table that match our schema
   * - Empty string, load by position rather than column list
   */
  private def getColumnList: Either[ConnectorError, String] = {
    config.copyColumnList match {
      case Some(list) =>
        logger.info(s"Using custom COPY column list. " + "Target table: " + config.tablename.getFullTableName +
          ", " + "copy_column_list: " + list + ".")
        Right("(" + list + ")")
      case None =>
        // Default COPY
        // TODO: Implement this with append mode
        // Behavior should be default to load by position if we created the table
        // We know this by lack of custom create statement + lack of append mode
        //if ((params("target_table_ddl") == null) && (params("save_mode") != "Append")) {
        if(false) {
          //If connector created the target table no need to try to load by name, except for Append mode
          logger.info("Load by Position")
          Right("")
        }
        else {
          logger.info(s"Building default copy column list")
          schemaTools.getCopyColumnList(jdbcLayer, config.tablename.getFullTableName, config.schema) match {
            case Left(err) =>
              logger.error("Schema tools error: " + err.msg)
              Left(ConnectorError(SchemaColumnListError))
            case Right(str) => Right(str)
          }
        }
    }
  }

  private case class FaultToleranceTestResult(success: Boolean, failedRowsPercent: Double)

  private def testFaultTolerance(rowsCopied: Int, rejectsTable: String) : Either[JDBCLayerError, FaultToleranceTestResult] = {
    // verify rejects to see if this falls within user tolerance.
    val rejectsQuery = "SELECT COUNT(*) as count FROM " + rejectsTable
    logger.info(s"Checking number of rejected rows via statement: " + rejectsQuery)

    for {
      rs <- jdbcLayer.query(rejectsQuery)
      rejectedCount = if (rs.next) {
          rs.getInt("count")
        }
        else {
          logger.error("Could not retrieve rejected row count.")
          0
        }

      failedRowsPercent = {
        if (rowsCopied > 0) rejectedCount.toDouble / (rowsCopied.toDouble + rejectedCount.toDouble)
        else 1.0
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
        if(passedFaultToleranceTest) "...PASSED.  OK to commit to database."
        else "...FAILED.  NOT OK to commit to database"
      })

      _ <- if(rejectedCount == 0) {
        val dropRejectsTableStatement = "DROP TABLE IF EXISTS " + rejectsTable  + " CASCADE"
        logger.info(s"Dropping Vertica rejects table now: " + dropRejectsTableStatement)
        jdbcLayer.execute(dropRejectsTableStatement)
      } else Right(())

      testResult = FaultToleranceTestResult(passedFaultToleranceTest, failedRowsPercent)
    } yield testResult
  }

  def commit(): Either[ConnectorError, Unit] = {
    val globPattern: String = "*.parquet"
    val url: String = s"${config.fileStoreConfig.address.stripSuffix("/")}/$globPattern"

    val ret = for {
      // Get columnList
      columnList <- getColumnList

      tableName = config.tablename.name
      sessionId = config.sessionId
      rejectsTableName = tableName.substring(0,Math.min(30,tableName.length)) + "_" + sessionId + "_COMMITS"

      copyStatement = buildCopyStatement(config.tablename.getFullTableName,
        columnList,
        url,
        rejectsTableName,
        "parquet"
      )

      rowsCopied <- jdbcLayer.executeUpdate(copyStatement) match {
        case Right(v) =>
          fileStoreLayer.removeDir(config.fileStoreConfig.address)
          Right(v)
        case Left (err) =>
          logger.error ("JDBC Error when trying to copy data into Vertica: " + err.msg)
          fileStoreLayer.removeDir (config.fileStoreConfig.address)
          Left(ConnectorError(CommitError))
      }

      faultToleranceResults <- testFaultTolerance(rowsCopied, rejectsTableName) match {
        case Right (b) => Right (b)
        case Left (err) =>
          logger.error ("JDBC Error when trying to determine fault tolerance: " + err.msg)
          Left(ConnectorError(CommitError))
      }

      _ <- tableUtils.updateJobStatusTable(config.tablename, config.jdbcConfig.username, faultToleranceResults.failedRowsPercent, config.sessionId, faultToleranceResults.success)

      _ <- if(faultToleranceResults.success) {
            Right(())
          }
          else {
            Left(ConnectorError(FaultToleranceTestFail))
          }
    } yield ()

    // Commit or rollback
    val result = ret match {
      case Right(_) =>
        jdbcLayer.commit() match {
          case Right(()) => Right(())
          case Left(err) =>
            logger.error ("JDBC Error when trying to commit: " + err.msg)
            Left(ConnectorError(CommitError))
        }
      case Left(err) =>
        jdbcLayer.rollback() match {
          case Right(()) => ()
          case Left(err) => logger.error ("JDBC Error when trying to rollback: " + err.msg)
        }
        Left(err)
    }

    jdbcLayer.close()
    result
  }
}
