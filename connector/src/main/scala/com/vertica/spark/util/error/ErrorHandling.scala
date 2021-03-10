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

import cats.data.NonEmptyList

trait ConnectorError {
  // Adds context to an error
  def context(text: String): ConnectorError = ContextError(text, this)

  // Gets the context for debugging. This context is more helpful for developers.
  def getFullContext: String

  // Get the underlying error object. This can be used to determine what kind of error occurred.
  def getError: ConnectorError

  // Gets a user-friendly error message.
  def getUserMessage: String
}

case class ContextError(ctxt: String, error: ConnectorError) extends ConnectorError {
  def getFullContext: String = this.ctxt + "\n" + error.getFullContext
  def getError: ConnectorError = this.error.getError
  def getUserMessage: String = this.error.getUserMessage
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
}

case class SchemaDiscoveryError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Failed to discover the schema of the table. " +
    "There may be an issue with connectivity to the database."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  def getError: ConnectorError = this
  def getUserMessage: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getUserMessage)
    case None => this.message
  }
}
case class SchemaColumnListError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to create a valid column list for the write operation " +
    "due to mismatch with the existing table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class SchemaConversionError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to convert the schema of the table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ExportFromVerticaError(error: ConnectorError) extends ConnectorError {
  private val message = "There was an error when attempting to export from Vertica: " +
    "connection error with JDBC."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ParquetMetadataError() extends ConnectorError {
  private val message = "There was an error retrieving parquet file metadata."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class PartitioningError() extends ConnectorError {
  private val message = "Failure when retrieving partitioning information for operation."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidPartition() extends ConnectorError {
  private val message = "Input Partition was not valid for the given operation."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}

// TODO: Remove
case class DoneReading() extends ConnectorError {
  private val message = "No more data to read from source."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}

case class UninitializedReadCloseError() extends ConnectorError {
  private val message = "Error while closing read: The reader was not initialized."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class UninitializedReadError() extends ConnectorError {
  private val message = "Error while reading: The reader was not initialized."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class MissingMetadata() extends ConnectorError {
  private val message = "Metadata was missing from config."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class TooManyPartitions() extends ConnectorError {
  private val message = "More partitions specified than is possible to divide the operation."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CastingSchemaReadError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to get table schema when checking for fields that need casts."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CleanupError() extends ConnectorError {
  private val message = "Unexpected error when attempting to clean up files."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class MissingSchemaError() extends ConnectorError {
  private val message = "Expected to be passed in schema for this configuration. No schema found."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class TableCheckError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error checking if table exists: connection error with JDBC."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CreateTableError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error when trying to create table. Check 'target_table_sql' option for issues."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class DropTableError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Error when trying to drop table. Check 'target_table_sql' option for issues."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CommitError(error: ConnectorError) extends ConnectorError {
  private val message = "Error in commit step of write to Vertica. " +
    "There was a failure copying data from the intermediary into Vertica."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ViewExistsError() extends ConnectorError {
  private val message = "Table name provided cannot refer to an existing view in Vertica."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class TempTableExistsError() extends ConnectorError {
  private val message = "Table name provided cannot refer to a temporary tt"

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class FaultToleranceTestFail() extends ConnectorError {
  private val message = "Failed row count is above error tolerance threshold. Operation aborted."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class JobStatusCreateError(error: ConnectorError) extends ConnectorError {
  private val message = "Failed to create job status table."

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class JobStatusUpdateError(error: Option[ConnectorError]) extends ConnectorError {
  private val message = "Failed to update job status table."

  def getFullContext: String = this.error match {
    case Some(err) => ErrorHandling.appendErrors(this.message, err.getFullContext)
    case None => this.message
  }
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class DuplicateColumnsError() extends ConnectorError {
  private val message = "Schema contains duplicate columns, can't write this data."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidLoggingLevel() extends ConnectorError {
  private val message = "logging_level is incorrect. Use ERROR, INFO, DEBUG, or WARNING instead."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ConfigBuilderError() extends ConnectorError {
  private val message = "There was an unexpected problem building the configuration object. " +
    "Mandatory value missing."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class HostMissingError() extends ConnectorError {
  private val message = "The 'host' param is missing. Please specify the IP address " +
    "or hostname of the Vertica server to connect to."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class DbMissingError() extends ConnectorError {
  private val message = "The 'db' param is missing. Please specify the name of the Vertica " +
    "database to connect to."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class UserMissingError() extends ConnectorError {
  private val message = "The 'user' param is missing. Please specify the username to use " +
    "for authenticating with Vertica."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class PasswordMissingError() extends ConnectorError {
  private val message = "The 'password' param is missing. Please specify the password to use " +
    "for authenticating with Vertica."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class TablenameMissingError() extends ConnectorError {
  private val message = "The 'table' param is missing. Please specify the name of the table to use."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidPortError() extends ConnectorError {
  private val message = "The 'port' param specified is invalid. " +
    "Please specify a valid integer between 1 and 65535."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidFailedRowsTolerance() extends ConnectorError {
  private val message = "The 'failed_rows_percent_tolerance' param specified is invalid. " +
    "Please specify ad valid float between 0 and 1, representing a percent between 0 and 100."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidStrlenError() extends ConnectorError {
  private val message = "The 'strlen' param specified is invalid. " +
    "Please specify a valid integer between 1 and 32000000."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class InvalidPartitionCountError() extends ConnectorError {
  private val message = "The 'num_partitions' param specified is invalid. " +
    "Please specify a valid integer above 0."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class StagingFsUrlMissingError() extends ConnectorError {
  private val message = "The 'staging_fs_url' option is missing. " +
    "Please specify the url of the filesystem to use as an intermediary storage location between spark and Vertica."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class FileSystemError() extends ConnectorError {
  private val message = "There was an error communicating with the intermediary filesystem."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class FileListError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error listing files in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CreateFileError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating a file in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CreateDirectoryError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error creating a directory in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class RemoveFileError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing the specified file in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class RemoveDirectoryError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error removing the specified directory in the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class RemoveFileDoesNotExistError() extends ConnectorError {
  private val message = "The specified file to remove does not exist in the intermediary filesystem."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class RemoveDirectoryDoesNotExistError() extends ConnectorError {
  private val message = "The specified directory to remove does not exist in the intermediary filesystem."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CreateFileAlreadyExistsError() extends ConnectorError {
  private val message = "The specified file to create already exists in the intermediary filesystem."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CreateDirectoryAlreadyExistsError() extends ConnectorError {
  private val message = "The specified directory to create already exists in the intermediary filesystem."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class OpenWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class IntermediaryStoreWriterNotInitializedError() extends ConnectorError {
  private val message = "Intermediary filesystem write error: The writer was not initialized."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class IntermediaryStoreWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error writing to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CloseWriteError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the write to the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class OpenReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error opening a read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class IntermediaryStoreReaderNotInitializedError() extends ConnectorError {
  private val message = "Intermediary filesystem read error: The reader was not initialized."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class IntermediaryStoreReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error reading from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class CloseReadError(cause: Throwable) extends ConnectorError {
  private val message = "There was an error closing the read from the intermediary filesystem."

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ErrorList(errors: NonEmptyList[ConnectorError]) extends ConnectorError {
  def getFullContext: String = this.errors.toList.map(errs => errs.getFullContext).mkString("\n")
  def getError: ConnectorError = this
  def getUserMessage: String = this.errors.toList.map(errs => errs.getUserMessage).mkString("\n")
}

/**
  * Enumeration of the list of possible JDBC errors.
  */
trait JdbcError extends ConnectorError

case class ConnectionError() extends JdbcError {
  private val message = "Failed to connect to the JDBC source"

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class ConnectionDownError() extends JdbcError {
  private val message = "Connection to the JDBC source is down or invalid"

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class DataTypeError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: Wrong data type"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class SyntaxError(cause: Throwable) extends JdbcError {
  private val message = "JDBC Error: syntax error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message + ": " + this.cause.toString
}
case class GenericError(cause: Throwable) extends JdbcError {
  private val message = "Generic JDBC error occurred"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message + ": " + this.cause.toString
}

/**
  * Enumeration of the list of possible schema errors.
  */
trait SchemaError extends ConnectorError

case class MissingConversionError(value: String) extends SchemaError {
  private val message = "Could not find conversion for unsupported SQL type: " + value

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class UnexpectedExceptionError(cause: Throwable) extends SchemaError {
  private val message = "Unexpected exception while retrieving schema"

  def getFullContext: String = ErrorHandling.addCause(this.message, this.cause)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class JdbcSchemaError(error: ConnectorError) extends SchemaError {
  private val message = "JDBC failure when trying to retrieve schema"

  def getFullContext: String = ErrorHandling.appendErrors(this.message, error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
case class TableNotEnoughRowsError() extends SchemaError {
  private val message = "Attempting to write to a table with less columns than the spark schema."

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}
