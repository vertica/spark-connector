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
  type Result[E, T] = Either[E, T]

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

  def appendErrors(errorText1: String, errorText2: String): String = {
    errorText1 + "\n" + errorText2
  }

  def logAndThrowError(logger: Logger, error: ConnectorError): Nothing = {
    logger.error(error.getFullContext)
    throw new ConnectorException(error)
  }
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
    case None => this.message
  }
}
case class SchemaColumnListError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to create a valid column list for the write operation " +
    "due to mismatch with the existing table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}
case class SchemaConversionError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to convert the schema of the table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}
case class ExportFromVerticaError(error: ConnectorError) extends ConnectorError {
  private val message = "There was an error when attempting to export from Vertica: " +
    "connection error with JDBC."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}
case class ParquetMetadataError() extends ConnectorError {
  def getFullContext: String = "There was an error retrieving parquet file metadata."
}
case class PartitioningError() extends ConnectorError {
  def getFullContext: String = "Failure when retrieving partitioning information for operation."
}
case class InvalidPartition() extends ConnectorError {
  def getFullContext: String = "Input Partition was not valid for the given operation."
}

// TODO: Remove
case class DoneReading() extends ConnectorError {
  def getFullContext: String = "No more data to read from source."
}

case class UninitializedReadCloseError() extends ConnectorError {
  def getFullContext: String = "Error while closing read: The reader was not initialized."
}
case class UninitializedReadError() extends ConnectorError {
  def getFullContext: String = "Error while reading: The reader was not initialized."
}
case class MissingMetadata() extends ConnectorError {
  def getFullContext: String = "Metadata was missing from config."
}
case class TooManyPartitions() extends ConnectorError {
  def getFullContext: String = "More partitions specified than is possible to divide the operation."
}
case class CastingSchemaReadError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to get table schema when checking for fields that need casts."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, error.getFullContext)
  override def getUserMessage: String = this.message
}
case class CleanupError() extends ConnectorError {
  def getFullContext: String = "Unexpected error when attempting to clean up files."
}
case class MissingSchemaError() extends ConnectorError {
  def getFullContext: String = "Expected to be passed in schema for this configuration. No schema found."
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
    "for authenticating with Vertica."
}
case class PasswordMissingError() extends ConnectorError {
  def getFullContext: String = "The 'password' param is missing. Please specify the password to use " +
    "for authenticating with Vertica."
}
case class TablenameMissingError() extends ConnectorError {
  def getFullContext: String = "The 'table' param is missing. Please specify the name of the table to use."
}
case class InvalidPortError() extends ConnectorError {
  def getFullContext: String = "The 'port' param specified is invalid. " +
    "Please specify a valid integer between 1 and 65535."
}
case class UnquotedSemiInColumns() extends ConnectorError {
  def getFullContext: String = "Column list contains unquoted semicolon. Not accepted due to potential SQL injection vulnerability."
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
case class FileSystemError() extends ConnectorError {
  def getFullContext: String = "There was an error communicating with the intermediary filesystem."
}
case class FileListError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error listing files in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class CreateFileError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating a file in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class CreateDirectoryError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating a directory in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class RemoveFileError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing the specified file in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class RemoveDirectoryError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing the specified directory in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class RemoveFileDoesNotExistError() extends ConnectorError {
  def getFullContext: String = "The specified file to remove does not exist in the intermediary filesystem."
}
case class RemoveDirectoryDoesNotExistError() extends ConnectorError {
  def getFullContext: String = "The specified directory to remove does not exist in the intermediary filesystem."
}
case class CreateFileAlreadyExistsError() extends ConnectorError {
  def getFullContext: String = "The specified file to create already exists in the intermediary filesystem."
}
case class CreateDirectoryAlreadyExistsError() extends ConnectorError {
  def getFullContext: String = "The specified directory to create already exists in the intermediary filesystem."
}
case class OpenWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class IntermediaryStoreWriterNotInitializedError() extends ConnectorError {
  def getFullContext: String = "Intermediary filesystem write error: The writer was not initialized."
}
case class IntermediaryStoreWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error writing to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class CloseWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class OpenReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class IntermediaryStoreReaderNotInitializedError() extends ConnectorError {
  def getFullContext: String = "Intermediary filesystem read error: The reader was not initialized."
}
case class IntermediaryStoreReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error reading from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class CloseReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class ErrorList(errors: NonEmptyList[ConnectorError]) extends ConnectorError {
  def getFullContext: String = this.errors.toList.map(errs => errs.getFullContext).mkString("\n")
  override def getUserMessage: String = this.errors.toList.map(errs => errs.getUserMessage).mkString("\n")
}

/**
  * Enumeration of the list of possible JDBC errors.
  */
trait JdbcError extends ConnectorError

case class ConnectionError() extends JdbcError {
  def getFullContext: String = "Failed to connect to the JDBC source"
}
case class ConnectionDownError() extends JdbcError {
  def getFullContext: String = "Connection to the JDBC source is down or invalid"
}
case class DataTypeError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: Wrong data type"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class SyntaxError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: syntax error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class GenericError(cause: Throwable) extends JdbcError {
  private val message = "Generic JDBC error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class ParamsNotSupported(operation: String) extends JdbcError {
  private val message = "Params not supported for operation: " + operation

  def getFullContext: String = message
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
case class UnexpectedExceptionError(cause: Throwable) extends SchemaError {
  private val message = "Unexpected exception while retrieving schema"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  override def getUserMessage: String = this.message
}
case class JdbcSchemaError(error: ConnectorError) extends SchemaError {
  private val message = "JDBC failure when trying to retrieve schema"

  def getFullContext: String = ErrorHandling.appendErrors(this.message, error.getFullContext)
  override def getUserMessage: String = this.message
}
case class TableNotEnoughRowsError() extends SchemaError {
  def getFullContext: String = "Attempting to write to a table with less columns than the spark schema."
}
