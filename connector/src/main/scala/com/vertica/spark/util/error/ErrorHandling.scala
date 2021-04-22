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

package com.vertica.spark.util.error

/**
  * Enumeration of the list of possible connector errors.
  */

import cats.data.NonEmptyList
import com.typesafe.scalalogging.Logger
import com.vertica.spark.util.error.ErrorHandling.invariantViolation
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.DataType

trait ConnectorError {
  // Adds context to an error
  def context(text: String): ConnectorError = ContextError(text, this)

  // Gets the context for debugging. This context is more helpful for developers.
  def getFullContext: String

  // Get the underlying error object. This can be used to determine what kind of error occurred.
  def getError: ConnectorError = this

  // Gets a user-friendly error message.
  def getUserMessage: String = this.getFullContext
}

case class ContextError(ctxt: String, error: ConnectorError) extends ConnectorError {
  def getFullContext: String = this.ctxt + "\n" + error.getFullContext
  override def getError: ConnectorError = this.error.getError
  override def getUserMessage: String = this.error.getUserMessage
}


class ConnectorException(val error: ConnectorError) extends Exception {
  override def getMessage: String = this.error.getUserMessage
}

object ErrorHandling {
  type Result[+E, +T] = Either[E, T]

  type ConnectorResult[T] = Result[ConnectorError, T]
  type SchemaResult[T] = Result[SchemaError, T]
  type JdbcResult[T] = Result[JdbcError, T]

  def addCause(errorText: String, throwable: Throwable): String = {
    errorText +
    "\n\nCaused by:\n" +
    throwable.toString +
    "\nStack trace:\n" +
    throwable.getStackTrace.mkString("\n")
  }

  def addUserFriendlyCause(errorText: String, throwable: Throwable): String = {
    errorText + "\nCaused by: " + throwable.getMessage
  }

  def appendErrors(errorText1: String, errorText2: String): String = {
    errorText1 + "\n" + errorText2
  }

  def logAndThrowError(logger: Logger, error: ConnectorError): Nothing = {
    logger.error(error.getFullContext)
    throw new ConnectorException(error)
  }

  val invariantViolation: String = "This is likely a bug and should be reported to the developers here:\n" +
    "https://github.com/vertica/spark-connector/issues"
}

case class SchemaDiscoveryError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Failed to discover the schema of the table. " +
    "There may be an issue with connectivity to the database."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  override def getUserMessage: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getUserMessage)
    case None => "Failed to discover the schema of the table. " + invariantViolation
  }
}
case class SchemaColumnListError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to create a valid column list for the write operation " +
    "due to mismatch with the existing table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = ErrorHandling.appendErrors(this.message, this.error.getUserMessage)
}
case class SchemaConversionError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to convert the schema of the table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = ErrorHandling.appendErrors(this.message, this.error.getUserMessage)
}
case class ExportFromVerticaError(error: ConnectorError) extends ConnectorError {
  private val message = "There was an error when attempting to export from Vertica: " +
    "connection error with JDBC."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = ErrorHandling.appendErrors(this.message, this.error.getUserMessage)
}
case class InitialSetupPartitioningError() extends ConnectorError {
  def getFullContext: String = "Failure when retrieving partitioning information for operation.\n" + invariantViolation
}
case class FileListEmptyPartitioningError() extends ConnectorError {
  def getFullContext: String = "Failure when retrieving partitioning information for operation. " +
    "The returned file list was empty, so valid partition info cannot be created."
}
case class InvalidPartition() extends ConnectorError {
  def getFullContext: String = "Input Partition was not valid for the given operation."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}

// TODO: Remove
case class DoneReading() extends ConnectorError {
  def getFullContext: String = "No more data to read from source."
}

case class UninitializedReadError() extends ConnectorError {
  def getFullContext: String = "Error while reading: The reader was not initialized."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}
case class MissingMetadata() extends ConnectorError {
  def getFullContext: String = "Metadata was missing from config."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}
case class CleanupError(path: String) extends ConnectorError {
  def getFullContext: String = "Unexpected error when attempting to clean up files. " +
    "Parent directory missing for path: " + path
}
case class MissingSchemaError() extends ConnectorError {
  def getFullContext: String = "Expected to be passed in schema for this configuration. No schema found."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}
case class TableCheckError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error checking if table exists: connection error with JDBC."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  override def getUserMessage: String = this.message
}
case class CreateTableError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error when trying to create table. Check 'target_table_sql' option for issues."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  override def getUserMessage: String = this.message
}
case class DropTableError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error when trying to drop table. Check 'target_table_sql' option for issues."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  override def getUserMessage: String = this.message
}
case class CommitError(error: ConnectorError) extends ConnectorError {
  private val message = "Error in commit step of write to Vertica. " +
    "There was a failure copying data from the intermediary into Vertica."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}
case class ViewExistsError() extends ConnectorError {
  def getFullContext: String = "Table name provided cannot refer to an existing view in Vertica."
}
case class TempTableExistsError() extends ConnectorError {
  def getFullContext: String = "Table name provided cannot refer to a temporary tt"
}
case class SetSparkConfError(cause: Throwable) extends ConnectorError {
  private val message = "Error setting spark configuration. "

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class FaultToleranceTestFail() extends ConnectorError {
  def getFullContext: String = "Failed row count is above error tolerance threshold. Operation aborted."
}
case class JobStatusCreateError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to create job status table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}
case class JobStatusUpdateError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Failed to update job status table."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  override def getUserMessage: String = this.message
}
case class DuplicateColumnsError() extends ConnectorError {
  def getFullContext: String = "Schema contains duplicate columns, can't write this data."
}
case class InvalidLoggingLevel() extends ConnectorError {
  def getFullContext: String = "logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead."
}
case class ConfigBuilderError() extends ConnectorError {
  def getFullContext: String = "There was an unexpected problem building the configuration object. " +
    "Mandatory value missing."
}
case class HostMissingError() extends ConnectorError {
  def getFullContext: String = "The 'host' param is missing. Please specify the IP address " +
    "or hostname of the Vertica server to connect to."
}
case class DbMissingError() extends ConnectorError {
  def getFullContext: String = "The 'db' param is missing. Please specify the name of the Vertica " +
    "database to connect to."
}
case class UserMissingError() extends ConnectorError {
  def getFullContext: String = "The 'user' param is missing. Please specify the username to use " +
    "for authenticating with Vertica, as well as authentication details.."
}
case class PasswordMissingError() extends ConnectorError {
  def getFullContext: String = "The 'password' param is missing. Please specify the password to use " +
    "for authenticating with Vertica."
}
case class KerberosAuthMissingError() extends ConnectorError {
  def getFullContext: String = "Some Kerberos authentication details are missing. Please specify the following parameters:" +
    " 'kerberos_service_name', 'kerberos_host_name', 'jaas_config_name'"
}
case class TablenameMissingError() extends ConnectorError {
  def getFullContext: String = "The 'table' param is missing. Please specify the name of the table to use."
}
case class TableAndQueryMissingError() extends ConnectorError {
  def getFullContext: String = "The 'table' and 'query' params are both missing. Please specify the table name or query to use."
}
case class QuerySpecifiedOnWriteError() extends ConnectorError {
  def getFullContext: String = "The 'query' option was specified for a write operation. This option is only valid for reads."
}
case class InvalidPortError() extends ConnectorError {
  def getFullContext: String = "The 'port' param specified is invalid. " +
    "Please specify a valid integer between 1 and 65535."
}
case class InvalidIntegerField(field: String) extends ConnectorError {
  def getFullContext: String = "Field '" + field + "' is not a valid integer."
}
case class UnquotedSemiInColumns() extends ConnectorError {
  def getFullContext: String = "Column list contains unquoted semicolon. Not accepted due to potential SQL injection vulnerability."
}
case class InvalidFilePermissions() extends ConnectorError {
  def getFullContext: String = "File permissions are not in the correct format. Please specify a three digit number representing the file perms, ie 777 or 750."
}
case class InvalidFailedRowsTolerance() extends ConnectorError {
  def getFullContext: String = "The 'failed_rows_percent_tolerance' param specified is invalid. " +
    "Please specify ad valid float between 0 and 1, representing a percent between 0 and 100."
}
case class InvalidStrlenError() extends ConnectorError {
  def getFullContext: String = "The 'strlen' param specified is invalid. " +
    "Please specify a valid integer between 1 and 32000000."
}
case class InvalidPartitionCountError() extends ConnectorError {
  def getFullContext: String = "The 'num_partitions' param specified is invalid. " +
    "Please specify a valid integer above 0."
}
case class StagingFsUrlMissingError() extends ConnectorError {
  def getFullContext: String = "The 'staging_fs_url' option is missing. " +
    "Please specify the url of the filesystem to use as an intermediary storage location between spark and Vertica."
}
case class ParentDirMissingError(path: String) extends ConnectorError {
  def getFullContext: String = "Could not retrieve parent path of file: " + this.path
}
case class FileListError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error listing files in the intermediary filesystem"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class CreateFileError(path: Path, cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating file " + this.path.toString + " in the intermediary filesystem"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class CreateDirectoryError(path: Path, cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating directory " + this.path.toString + " in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class RemoveFileError(path: Path, cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing file " + this.path.toString + " in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class RemoveDirectoryError(path: Path, cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing the directory " + this.path.toString + " in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
case class CreateFileAlreadyExistsError(filename: String) extends ConnectorError {
  def getFullContext: String = "Error creating file " + this.filename +
    ". The file already exists in the intermediary filesystem."
}
case class CreateDirectoryAlreadyExistsError(filename: String) extends ConnectorError {
  def getFullContext: String = "Error creating directory " + this.filename +
    ". The directory already exists in the intermediary filesystem."
}
case class OpenWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, this.cause)
}
case class IntermediaryStoreWriterNotInitializedError() extends ConnectorError {
  def getFullContext: String = "Intermediary filesystem write error: The writer was not initialized."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}
case class IntermediaryStoreWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error writing to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, this.cause)
}
case class CloseWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + cause.getMessage
}
case class OpenReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + cause.getMessage
}
case class IntermediaryStoreReaderNotInitializedError() extends ConnectorError {
  def getFullContext: String = "Intermediary filesystem read error: The reader was not initialized."
  override def getUserMessage: String = ErrorHandling.appendErrors(this.getFullContext, invariantViolation)
}
case class IntermediaryStoreReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error reading from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + cause.getMessage
}
case class CloseReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + cause.getMessage
}
case class ErrorList(errors: NonEmptyList[ConnectorError]) extends ConnectorError {
  def getFullContext: String = this.errors.toList.map(errs => errs.getFullContext).mkString("\n")
  override def getUserMessage: String = this.errors.toList.map(errs => errs.getUserMessage).mkString("\n")
}

/**
  * Enumeration of the list of possible JDBC errors.
  */
trait JdbcError extends ConnectorError

case class ConnectionSqlError(cause: Throwable) extends JdbcError {
  private val message = "A JDBC SQL exception occurred while trying to connect to Vertica. " +
    "Check the JDBC URI and properties to see if they are correct."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + "\nCause: " + this.cause.getMessage
}
case class ConnectionError(cause: Throwable) extends JdbcError {
  private val message = "An unknown JDBC exception occurred while trying to connect to Vertica. " +
    "Check the JDBC URI and properties to see if they are correct."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + "\nCause: " + this.cause.getMessage
}
case class ConnectionDownError() extends JdbcError {
  def getFullContext: String = "Connection to the JDBC source is down or invalid. " +
    "Please ensure that the JDBC source is running properly."
}
case class DataTypeError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: Wrong data type"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class SyntaxError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: A syntax error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class GenericError(cause: Throwable) extends JdbcError {
  private val message = "A generic JDBC error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class ParamsNotSupported(operation: String) extends JdbcError {
  def getFullContext: String = "Params not supported for operation: " + operation
}

/**
  * Enumeration of the list of possible schema errors.
  */
trait SchemaError extends ConnectorError

case class MissingSqlConversionError(sqlType: String, typename: String) extends SchemaError {
  def getFullContext: String = "Could not find conversion for unsupported SQL type: " + typename +
    "\nSQL type value: " + sqlType
}
case class MissingSparkConversionError(sparkType: DataType) extends SchemaError {
  def getFullContext: String = "Could not find conversion for unsupported Spark type: " + sparkType.typeName
}
case class DatabaseReadError(cause: Throwable) extends SchemaError {
  def getFullContext: String = ErrorHandling.addCause("Exception while retrieving schema.", this.cause)
  override def getUserMessage: String = "Could not read from database: " + cause.getMessage
}
case class JdbcSchemaError(error: ConnectorError) extends SchemaError {
  private val message = "JDBC failure when trying to retrieve schema"

  def getFullContext: String = ErrorHandling.appendErrors(this.message, error.getFullContext)
  override def getUserMessage: String = this.message
}
case class TableNotEnoughRowsError() extends SchemaError {
  def getFullContext: String = "Attempting to write to a table with less columns than the spark schema."
}

case class MissingHDFSImpersonationTokenError(username: String, address: String) extends ConnectorError {
  override def getFullContext: String = "Could not retrieve an impersonation token for the desginated user " + username + " on address: " + address
}

case class KerberosNotEnabledInHadoopConf() extends ConnectorError {
  override def getFullContext: String = "Trying to use Kerberos, but did not detect hadoop configuration with Kerberos enabled."
}

case class NoSparkSessionFound() extends ConnectorError {
  override def getFullContext: String = "Could not get spark session. " + invariantViolation
}
case class FileStoreThrownError(cause: Throwable) extends ConnectorError {
  private val message = "Unexpected error in interaction with filestore. "

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = ErrorHandling.addUserFriendlyCause(this.message, cause)
}
