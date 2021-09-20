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

import com.vertica.spark.datasource.v2.{ExpectedRowDidNotExistError}
import com.vertica.spark.util.general.Utils
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.types.IntegerType

import scala.util.{Failure, Success, Try}

trait TestError extends ConnectorError


case class MyError() extends TestError {
  def getFullContext: String = "My test error"
}

case class InterceptError(error: ConnectorError) extends TestError {
  private val message = "Intercept test error"

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  override def getUserMessage: String = this.message
}

case class StackTraceError(cause: Throwable) extends TestError {
  def getFullContext: String = ErrorHandling.addCause("stack trace test: ", this.cause)
}

class ErrorHandlingTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  def addContextA(): ConnectorError = {
    addContextB().context("Failure when calling addContextA")
  }

  def addContextB(): ConnectorError = {
    myError().context("Failure when calling addContextB")
  }

  def myError(): ConnectorError = {
    MyError()
  }

  def interceptError(): ConnectorError = {
    InterceptError(addContextA())
  }

  def addContextC(): ConnectorError = {
    interceptError().context("Failure when calling addContextC")
  }

  def addCause(): Either[StackTraceError, Unit] = {
    Try { deferHandling() }.toEither.left.map(e => StackTraceError(e))
  }

  def deferHandling(): Nothing = {
    throwException()
  }

  def throwException(): Nothing = {
    throw new Exception("oh no")
  }

  it should "gather context for the returned error" in {
    assert(addContextA().getFullContext ==
      "Failure when calling addContextA\n" +
      "Failure when calling addContextB\n" +
      "My test error"
    )
  }

  it should "only print the user friendly error" in {
    assert(addContextA().getUserMessage == "My test error")
  }

  it should "allow downcasting to the TestError type" in {
    assert(addContextA().getError match {
      case _: TestError => true
      case _ => false
    })
  }

  it should "allow downcasting to the more specific MyError type" in {
    assert(addContextA().getError match {
      case MyError() => true
      case _ => false
    })
  }

  it should "catch a ConnectorException and allow downcasting to the specific error type" in {
    try {
      throw new ConnectorException(addContextA())
    } catch {
      case e: ConnectorException => assert(e.error.getError match {
        case MyError() => true
        case _ => false
      })
      case _: Exception => fail("The wrong exception occurred.")
    }
  }

  it should "catch a ConnectorException and get the underlying error's message" in {
    try {
      throw new ConnectorException(addContextA())
    } catch {
      case e: ConnectorException => assert(e.getMessage == "My test error")
      case _: Exception => fail("The wrong exception occurred.")
    }
  }

  it should "properly combine the intercepted error message and context with the new error" in {
    assert(addContextC().getFullContext ==
      "Failure when calling addContextC\n" +
      "Intercept test error\n" +
      "Failure when calling addContextA\n" +
      "Failure when calling addContextB\n" +
      "My test error"
    )
  }

  it should "replace the old error's user friendly error message with the new error's user friendly message" in {
    assert(addContextC().getUserMessage == "Intercept test error")
  }

  it should "still allow accessing the underlying error" in {
    addContextC().getError match {
      case InterceptError(err) => assert(err.getFullContext ==
        "Failure when calling addContextA\n" +
        "Failure when calling addContextB\n" +
        "My test error"
      )
      case _ => fail("Wrong error type returned")
    }
  }

  it should "properly add a stack trace to the error context" in {
    addCause() match {
      case Left(err) =>
        val errorContext = err.getFullContext
        assert(errorContext.startsWith("stack trace test: \n\nCaused by:\njava.lang.Exception: oh no\nStack trace:"))
        assert(errorContext.contains("throwException"))
        assert(errorContext.contains("deferHandling"))
      case Right(_) => fail
    }
  }

  it should "get the error messages from a SchemaColumnListError" in {
    val error = SchemaColumnListError(MyError())
    assert(error.getFullContext == "Failed to create a valid column list for the write operation " +
      "due to mismatch with the existing table.\nMy test error")
  }

  it should "get the error messages from a ExpectedRowDidNotExistError" in {
    val error = ExpectedRowDidNotExistError()
    assert(error.getFullContext == "Fatal error: expected row did not exist")
  }

  private def checkErrReturnsMessages(error: ConnectorError): Unit = {
    Utils.ignore(assert(error.getFullContext.length > 0))
    Utils.ignore(assert(error.getUserMessage.length > 0))
  }

  it should "return full context and user message for static errors without exception" in {
    Try {
      checkErrReturnsMessages(ExpectedRowDidNotExistError())
      checkErrReturnsMessages(DropTableError())
      checkErrReturnsMessages(InitialSetupPartitioningError())
      checkErrReturnsMessages(InvalidPartition())
      checkErrReturnsMessages(DoneReading())
      checkErrReturnsMessages(UninitializedReadError())
      checkErrReturnsMessages(MissingMetadata())
      checkErrReturnsMessages(MissingSchemaError())
      checkErrReturnsMessages(ViewExistsError())
      checkErrReturnsMessages(TempTableExistsError())
      checkErrReturnsMessages(FaultToleranceTestFail())
      checkErrReturnsMessages(DuplicateColumnsError())
      checkErrReturnsMessages(HostMissingError())
      checkErrReturnsMessages(DbMissingError())
      checkErrReturnsMessages(UserMissingError())
      checkErrReturnsMessages(PasswordMissingError())
      checkErrReturnsMessages(TablenameMissingError())
      checkErrReturnsMessages(InvalidPortError())
      checkErrReturnsMessages(InvalidPortError())
      checkErrReturnsMessages(UnquotedSemiInColumns())
      checkErrReturnsMessages(InvalidFilePermissions())
      checkErrReturnsMessages(InvalidFailedRowsTolerance())
      checkErrReturnsMessages(InvalidStrlenError())
      checkErrReturnsMessages(InvalidPartitionCountError())
      checkErrReturnsMessages(StagingFsUrlMissingError())
      checkErrReturnsMessages(IntermediaryStoreWriterNotInitializedError())
      checkErrReturnsMessages(IntermediaryStoreReaderNotInitializedError())
      checkErrReturnsMessages(ConnectionDownError())
      checkErrReturnsMessages(TableNotEnoughRowsError())
      checkErrReturnsMessages(SchemaDiscoveryError())
      checkErrReturnsMessages(MissingAWSSecretAccessKey())
      checkErrReturnsMessages(MissingAWSAccessKeyId())
      checkErrReturnsMessages(LoadConfigMissingSparkSessionError())
      checkErrReturnsMessages(NonEmptyDataFrameError())
      checkErrReturnsMessages(CreateExternalTableAlreadyExistsError())
      checkErrReturnsMessages(CreateExternalTableMergeKey())

    } match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }

  }

  it should "return full context and user message for errors with optional sub-error" in {
    Try {
      val suberr = TableNotEnoughRowsError()

      checkErrReturnsMessages(TableCheckError(Some(suberr)))
      checkErrReturnsMessages(TableCheckError(None))
      checkErrReturnsMessages(CreateTableError(Some(suberr)))
      checkErrReturnsMessages(CreateTableError(None))
      checkErrReturnsMessages(JobStatusUpdateError(Some(suberr)))
      checkErrReturnsMessages(JobStatusUpdateError(None))

    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }

  it should "return full context and user message for errors with mandatory sub-error" in {
    Try {
      val suberr = TableNotEnoughRowsError()

      checkErrReturnsMessages(SchemaColumnListError(suberr))
      checkErrReturnsMessages(SchemaConversionError(suberr))
      checkErrReturnsMessages(ExportFromVerticaError(suberr))
      checkErrReturnsMessages(CommitError(suberr))
      checkErrReturnsMessages(JobStatusCreateError(suberr))
      checkErrReturnsMessages(JdbcSchemaError(suberr))
      checkErrReturnsMessages(InferExternalTableSchemaError(suberr))
      checkErrReturnsMessages(MergeColumnListError(suberr))
    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }

  it should "return full context and user message for errors that take one string param" in {
    Try {
      val str = "ex;s@^$%&*%&@W($GDH"

      checkErrReturnsMessages(CleanupError(str))
      checkErrReturnsMessages(ParentDirMissingError(str))
      checkErrReturnsMessages(CreateFileAlreadyExistsError(str))
      checkErrReturnsMessages(CreateDirectoryAlreadyExistsError(str))
      checkErrReturnsMessages(ParamsNotSupported(str))
    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }

  it should "return full context and user message for errors that have a throwable cause" in {
    Try {
      val path = new Path("hdfs://test/path\\sdfa123$#*$&*&(#$.***")
      val cause = new Exception("test")

      checkErrReturnsMessages(FileListError(cause))
      checkErrReturnsMessages(CreateFileError(path, cause))
      checkErrReturnsMessages(CreateDirectoryError(path, cause))
      checkErrReturnsMessages(RemoveFileError(path, cause))
      checkErrReturnsMessages(RemoveDirectoryError(path, cause))
      checkErrReturnsMessages(IntermediaryStoreWriteError(cause))
      checkErrReturnsMessages(CloseWriteError(cause))
      checkErrReturnsMessages(OpenReadError(cause))
      checkErrReturnsMessages(IntermediaryStoreReadError(cause))
      checkErrReturnsMessages(CloseReadError(cause))
      checkErrReturnsMessages(ConnectionSqlError(cause))
      checkErrReturnsMessages(ConnectionError(cause))
      checkErrReturnsMessages(DataError(cause))
      checkErrReturnsMessages(SyntaxError(cause))
      checkErrReturnsMessages(GenericError(cause))
      checkErrReturnsMessages(DatabaseReadError(cause))
    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }

  it should "return full context and user message for conversion errors" in {
    Try {
      val sqlType = "invalid&#%$&*#%*&%#"
      val sparkType = IntegerType

      checkErrReturnsMessages(MissingSqlConversionError(sqlType, sqlType))
      checkErrReturnsMessages(MissingSparkConversionError(sparkType))
    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }

  it should "return full context and user message for errors that take two string params" in {
    Try {
      val firstStr = "dsfjnjs"
      val secondStr = "389rh#@$#Tldfn"

      checkErrReturnsMessages(V1ReplacementOption(firstStr, secondStr))
    }
    match {
      case Failure(e) => fail(e)
      case Success(_) => ()
    }
  }
}
