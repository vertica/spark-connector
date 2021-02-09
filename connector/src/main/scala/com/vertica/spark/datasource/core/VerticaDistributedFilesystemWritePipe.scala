package com.vertica.spark.datasource.core

import com.vertica.spark.config.{DistributedFilesystemWriteConfig, TableName, VerticaMetadata, VerticaWriteMetadata}
import com.vertica.spark.datasource.fs.FileStoreLayerInterface
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ConnectorErrorType.{CommitError, CreateTableError, SchemaConversionError, TableCheckError}
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.schema.SchemaToolsInterface

class VerticaDistributedFilesystemWritePipe(val config: DistributedFilesystemWriteConfig, val fileStoreLayer: FileStoreLayerInterface, val jdbcLayer: JdbcLayerInterface, val schemaTools: SchemaToolsInterface, val sessionIdProvider: SessionIdInterface = SessionId, val dataSize: Int = 1) extends VerticaPipeInterface with VerticaPipeWriteInterface {
  private val logger = config.logProvider.getLogger(classOf[VerticaDistributedFilesystemWritePipe])

  // No write metadata required for configuration as of yet
  def getMetadata: Either[ConnectorError, VerticaMetadata] = Right(VerticaWriteMetadata())

  def getDataBlockSize: Either[ConnectorError, Long] = Right(dataSize)

  private def tableExists(table: TableName, jdbcLayer: JdbcLayerInterface): Either[ConnectorError, Boolean] = {
    val dbschema = table.dbschema.getOrElse("public")
    val query = "select count(*) from v_catalog.tables where table_schema ILIKE '" +
      dbschema + "' and table_name ILIKE '" + table.name + "'"

    jdbcLayer.query(query) match {
      case Left(err) =>
        logger.error("JDBC Error when checking if table exists: ", err.msg)
        Left(ConnectorError(TableCheckError))
      case Right(rs) =>
        if(!rs.next()) {
          logger.error("Table check: empty result")
          Left(ConnectorError(TableCheckError))
        }
        else {
          Right(rs.getBoolean(1))
        }
    }
  }

  private def createTable(config: DistributedFilesystemWriteConfig): Either[ConnectorError, Unit] = {
    // Either get the user-supplied statement to create the table, or build our own
    val statement: String = config.targetTableSql match {
      case Some(sql) => sql
      case None =>
        val sb = new StringBuilder()
        sb.append("CREATE table \"" + config.tablename.getFullTableName + "\" (")

        var first = true
        config.schema.foreach(s => {
          logger.debug("colname=" + "\"" + s.name + "\"" + "; type=" + s.dataType + "; nullable="  + s.nullable)
          if (!first) { sb.append(",\n") }
          first = false
          sb.append("\"" + s.name + "\" ")

          // remains empty unless we have a DecimalType with precision/scale
          var decimal_qualifier: String = ""
          if (s.dataType.toString.contains("DecimalType")) {

            // has precision only
            val p = "DecimalType\\((\\d+)\\)".r
            if (s.dataType.toString.matches(p.toString)) {
              val p(prec) = s.dataType.toString
              decimal_qualifier = "(" + prec + ")"
            }

            // has precision and scale
            val ps = "DecimalType\\((\\d+),(\\d+)\\)".r
            if (s.dataType.toString.matches(ps.toString)) {
              val ps(prec,scale) = s.dataType.toString
              decimal_qualifier = "(" + prec + "," + scale + ")"
            }
          }

          val col = schemaTools.getVerticaTypeFromSparkType(s.dataType, config.strlen) match {
            case Left(err) =>
              logger.error("Schema error: " + err)
              return Left(ConnectorError(SchemaConversionError))
            case Right(datatype) => datatype + decimal_qualifier
          }
          sb.append(col)
          if (!s.nullable) { sb.append(" NOT NULL") }
        })

        sb.append(")  INCLUDE SCHEMA PRIVILEGES ")
        sb.toString
    }

    logger.debug(s"BUILDING TABLE WITH COMMAND: " + statement)
    jdbcLayer.execute(statement) match {
      case Right(_) => Right(())
      case Left(err) =>
        logger.error("JDBC Error creating table: " + err)
        Left(ConnectorError(CreateTableError))
    }
  }

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
      tableExistsPre <- tableExists(config.tablename, jdbcLayer)
      _ <- if(!tableExistsPre) createTable(config) else Right(())

      // Confirm table was created. This should only be false if the user specified an invalid target_table_sql
      tableExistsPost <- tableExists(config.tablename, jdbcLayer)
      _ <- if(tableExistsPost) Right(()) else Left(ConnectorError(CreateTableError))

      // Create the directory to export files to
      _ <- fileStoreLayer.createDir(config.fileStoreConfig.address)
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
    fileStoreLayer.closeWriteParquetFile()
  }


  def buildCopyStatement(targetTable: String, columnList: String, url: String, fileFormat: String): String = {
    s"COPY $targetTable $columnList FROM '$url' ON ANY NODE $fileFormat"
    // TODO: s"REJECTED DATA AS TABLE $rejectsTable NO COMMIT"
    // TODO: COMMIT AFTER CHECKING REJECTS / UPDATING STATUS
  }

  def commit(): Either[ConnectorError, Unit] = {
    val copyStatement = buildCopyStatement(config.tablename.getFullTableName,
      "", // TODO: Implement custom column copy list option
      config.fileStoreConfig.address,
      "parquet"
    )
    jdbcLayer.execute(copyStatement) match {
      case Right(_) => Right(())
      case Left(err) =>
        logger.error("JDBC Error when trying to copy data into Vertica: " + err.msg)
        Left(ConnectorError(CommitError))
    }
  }
}
