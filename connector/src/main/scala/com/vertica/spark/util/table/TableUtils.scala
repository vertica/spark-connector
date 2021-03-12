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

package com.vertica.spark.util.table

import com.vertica.spark.config.{LogProvider, TableName}
import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult
import com.vertica.spark.util.error.{ConnectorError, CreateTableError, DropTableError, JdbcError, JobStatusCreateError, JobStatusUpdateError, SchemaConversionError, TableCheckError}
import com.vertica.spark.util.schema.SchemaToolsInterface
import org.apache.spark.sql.types.StructType

trait TableUtilsInterface {
  def viewExists(view: TableName): ConnectorResult[Boolean]
  def tableExists(table: TableName): ConnectorResult[Boolean]
  def tempTableExists(table: TableName): ConnectorResult[Boolean]
  def createTable(tablename: TableName, targetTableSql: Option[String], schema: StructType, strlen: Long): ConnectorResult[Unit]
  def dropTable(tablename: TableName): ConnectorResult[Unit]
  def createAndInitJobStatusTable(tablename: TableName, user: String, sessionId: String): ConnectorResult[Unit]
  def updateJobStatusTable(tableName: TableName, user: String, failedRowsPercent: Double, sessionId: String, success: Boolean): ConnectorResult[Unit]
}

class TableUtils(logProvider: LogProvider, schemaTools: SchemaToolsInterface, jdbcLayer: JdbcLayerInterface) extends TableUtilsInterface {
  private val logger = logProvider.getLogger(classOf[TableUtils])

  override def tempTableExists(table: TableName): ConnectorResult[Boolean] = {
    val dbschema = table.dbschema.getOrElse("public")
    val query = " select is_temp_table as t from v_catalog.tables where table_name='" + table.name + "' and table_schema='" + dbschema + "'"
    val ret = for {
      rs <- jdbcLayer.query(query)
      isTemp = if (rs.next) {rs.getBoolean("t") } else false
      _ = rs.close()
    } yield isTemp

    ret.left.map(err => TableCheckError(Some(err)).context("Cannot append to a temporary table"))
  }

  override def viewExists(view: TableName): ConnectorResult[Boolean] = {
    val dbschema = view.dbschema.getOrElse("public")
    val query = "select count(*) from views where table_schema ILIKE '" +
      dbschema + "' and table_name ILIKE '" + view.name + "'"

    jdbcLayer.query(query) match {
      case Left(err) => Left(TableCheckError(Some(err)).context("JDBC Error when checking if view exists"))
      case Right(rs) =>
        if (!rs.next()) {
          Left(TableCheckError(None).context("View check: empty result"))
        } else {
          try {
            Right(rs.getInt(1) >= 1)
          } catch {
            case e: Throwable =>
              jdbcLayer.handleJDBCException(e)
              Left(TableCheckError(None))
          } finally {
            rs.close()
          }
        }
    }
  }

  override def tableExists(table: TableName): ConnectorResult[Boolean] = {
    val dbschema = table.dbschema.getOrElse("public")
    val query = "select count(*) from v_catalog.tables where table_schema ILIKE '" +
      dbschema + "' and table_name ILIKE '" + table.name + "'"

    jdbcLayer.query(query) match {
      case Left(err) => Left(TableCheckError(Some(err)).context("JDBC Error when checking if table exists"))
      case Right(rs) =>
        try {
          if (!rs.next()) {
            Left(TableCheckError(None).context("Table check: empty result"))
          } else {
            Right(rs.getInt(1) >= 1)
          }
        }
        catch {
          case e: Throwable =>
            jdbcLayer.handleJDBCException(e)
            Left(TableCheckError(None))
        }
        finally {
          rs.close()
        }
    }
  }

  override def createTable(tablename: TableName, targetTableSql: Option[String], schema: StructType, strlen: Long): ConnectorResult[Unit] = {
    // Either get the user-supplied statement to create the table, or build our own
    val statement: String = targetTableSql match {
      case Some(sql) => sql
      case None =>
        val sb = new StringBuilder()
        sb.append("CREATE table ")
        tablename.dbschema match {
          case Some(dbschema) =>
            sb.append("\"" + dbschema + "\"" + "." +
              "\"" + tablename.name + "\"")
          case None => sb.append("\"" + tablename.name + "\"")
        }
        sb.append(" (")

        var first = true
        schema.foreach(s => {
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

          for {
            col <- schemaTools.getVerticaTypeFromSparkType(s.dataType, strlen) match {
              case Left(err) =>
                Left(SchemaConversionError(err).context("Schema error when trying to create table"))
              case Right(datatype) => Right(datatype + decimal_qualifier)
            }
            _ = sb.append(col)
            _ = if (!s.nullable) { sb.append(" NOT NULL") }
          } yield ()
        })

        sb.append(")  INCLUDE SCHEMA PRIVILEGES ")
        sb.toString
    }

    logger.debug(s"BUILDING TABLE WITH COMMAND: " + statement)
    jdbcLayer.execute(statement).left.map(err => CreateTableError(Some(err)).context("JDBC Error creating table"))
  }

  def dropTable(tablename: TableName): ConnectorResult[Unit] = {
    jdbcLayer.execute("DROP TABLE IF EXISTS " + tablename.getFullTableName)
      .left.map(err => DropTableError(Some(err)).context("JDBC Error dropping table"))
  }

  override def createAndInitJobStatusTable(tablename: TableName, user: String, sessionId: String): ConnectorResult[Unit] = {
    val dbschema = tablename.dbschema match {
      case Some(schema) => schema
      case None => "public"
    }

    // Create job status table for the user if it doesn't exist
    val table = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase
    val createStatement = "CREATE TABLE IF NOT EXISTS \"" + dbschema + "\".\"" + table + "\"" +
      "(target_table_schema VARCHAR(128), " +
      "target_table_name VARCHAR(128), " +
      "save_mode VARCHAR(128), " +
      "job_name VARCHAR(256), " +
      "start_time TIMESTAMPTZ, " +
      "all_done BOOLEAN NOT NULL, " +
      "success BOOLEAN NOT NULL, " +
      "percent_failed_rows DOUBLE PRECISION)"

    val jobStartTime = java.util.Calendar.getInstance().getTime.toString
    val date = new java.util.Date()
    val timestamp = new java.sql.Timestamp(date.getTime)
    val randJobName = sessionId

    val comment = "COMMENT ON TABLE "  + dbschema + "." +  table + " IS 'Persistent job status table showing all jobs, serving as permanent record of data loaded from Spark to Vertica. Creation time:" + jobStartTime + "'"

    // TODO: handle save modes
    val insertStatement = "INSERT into " + dbschema + "." + table + " VALUES ('" + dbschema + "','" + tablename.name + "','" + "OVERWRITE" + "','" + randJobName +  "','" + timestamp + "'," + "false,false," + (-1.0).toString + ")"

    val ret = for {
      tableExists <- tableExists(TableName(table, Some(dbschema)))
      _ <- if(!tableExists) jdbcLayer.execute(createStatement) else Right(())
      _ <- if(!tableExists) jdbcLayer.execute(comment) else Right(())
      _ <- jdbcLayer.execute(insertStatement)
      _ <- jdbcLayer.commit()
    } yield ()

    ret match {
      case Left(err) => err.getError match {
        case er: JdbcError => Left(JobStatusCreateError(er)
          .context("JDBC error when trying to initialize job status table"))
        case _: ConnectorError => Left(err)
      }
      case Right(_) => Right(())
    }
  }

  override def updateJobStatusTable(mainTableName: TableName, user: String, failedRowsPercent: Double, sessionId: String, success: Boolean): ConnectorResult[Unit] = {
    val dbschema = mainTableName.dbschema.getOrElse("public")
    val tablename = "S2V_JOB_STATUS" + "_USER_" + user.toUpperCase

    val updateStatusTable = ("UPDATE "
      + dbschema + "." + tablename + " "
      + "SET all_done=" + true + ","
      + "success=" + success + ","
      + "percent_failed_rows=" + failedRowsPercent.toString + " "
      + "WHERE job_name='" + sessionId + "' "
      + "AND all_done=" + false)

    // update the S2V_JOB_STATUS table, and commit the final operation.
    logger.info(s"Updating " + dbschema + "." + tablename + " next...")
    jdbcLayer.executeUpdate(updateStatusTable) match {
      case Left(err) => Left(JobStatusUpdateError(Some(err)).context("JDBC Error when updating status table"))
      case Right(c) =>
        if (c == 1) {
          logger.info(s"Update of " + dbschema + "." + tablename + " succeeded.")
          Right(())
        } else {
          Left(JobStatusUpdateError(None).context("Status_table update failed."))
        }
    }
  }
}
