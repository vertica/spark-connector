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

package com.vertica.spark.datasource.v2

import cats.data.Validated.{Invalid, Valid}
import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.{LogProvider, WriteConfig, DistributedFilesystemWriteConfig}
import com.vertica.spark.datasource.core.{DSConfigSetupInterface, DSWriter, DSWriterInterface}
import com.vertica.spark.util.error.{ConnectorError, ErrorHandling, ErrorList}
import com.vertica.spark.util.error.{NonEmptyDataFrameError, JobAbortedError}
import com.vertica.spark.util.general.Utils
import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.catalyst.InternalRow

import collection.JavaConverters._

object WriteSucceeded extends WriterCommitMessage
object WriteFailed extends WriterCommitMessage

/**
  * Builds the class for use in writing to Vertica
  */
class VerticaWriteBuilder(info: LogicalWriteInfo, writeSetupInterface: DSConfigSetupInterface[WriteConfig]) extends WriteBuilder with SupportsTruncate {

  private val logger = LogProvider.getLogger(classOf[VerticaTable])
  private val config = writeSetupInterface.validateAndGetConfig(info.options.asScala.toMap) match {
    case Invalid(errList) =>
      ErrorHandling.logAndThrowError(logger, ErrorList(errList.toNonEmptyList))
    case Valid(cfg) => cfg
  }
  logger.debug("Config loaded")

/**
  * Builds the class representing a write operation to a Vertica table
  *
  * @return [[VerticaBatchWrite]]
  */
  override def buildForBatch(): BatchWrite = {
    new VerticaBatchWrite(config, writeSetupInterface)
  }

  def truncate: WriteBuilder = {
    config.setOverwrite(true)
    this
  }

}

/**
  * Represents a write operation to Vertica
  *
  * Extends mixin class to represent type of write. Options are Batch or Stream, we are doing a batch write.
  */
class VerticaBatchWrite(config: WriteConfig, writeSetupInterface: DSConfigSetupInterface[WriteConfig]) extends BatchWrite {
  private val logger: Logger = LogProvider.getLogger(classOf[VerticaBatchReader])

  // Perform initial setup for the write operation
  writeSetupInterface.performInitialSetup(config) match {
    case Left(err) => ErrorHandling.logAndThrowError(logger, err)
    case Right(_) => ()
  }


/**
  * Creates the writer factory which will be serialized and sent to workers
  *
  * @param physicalWriteInfo Structure containing partition information.
  * @return [[VerticaWriterFactory]]
  */
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new VerticaWriterFactory(config,)

/**
  * Responsible for commiting the write operation.
  *
  * @param writerCommitMessages list of commit messages returned from each worker node
  * Called after all worker nodes report that they have succesfully completed their operations.
  */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    val writer = new DSWriter(config, "")
    writer.commitRows() match {
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
      case Right(_) => ()
    }
  }

/**
  * Responsible for cleaning up a failed write operation.
  *
  * @param writerCommitMessages list of commit messages returned from each worker node
  * Called after one or more worker nodes report that they have failed.
  */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {
    ErrorHandling.logAndThrowError(logger, JobAbortedError())
  }
}

/**
  * Factory class for creating the Vertica writer
  *
  * This class is seriazlized and sent to each worker node. On the worker, createWriter will be called with a given unique id for the partition being written.
  */
class VerticaWriterFactory(config: WriteConfig) extends DataWriterFactory {

/**
  * Called from the worker node to get the writer for that node
  *
  * @param partitionId A unique identifier for the partition being written
  * @param taskId A unique identifier for the specific task, which there may be multiple of for a partition due to retries or speculative execution
  * @return [[VerticaBatchWriter]]
  */
  override def createWriter(partitionId: Int, taskId: Long): DataWriter[InternalRow] = {
    val uniqueId : String = partitionId + "-" + taskId
    val writer = new DSWriter(config, uniqueId)
    new VerticaBatchWriter(config, writer)
  }
}

/**
  * Writer class that passes rows to be written to the underlying datasource
  */
class VerticaBatchWriter(config: WriteConfig, writer: DSWriterInterface) extends DataWriter[InternalRow] {
  private val logger: Logger = LogProvider.getLogger(classOf[VerticaBatchReader])

  // Construct a unique identifier string based on the partition and task IDs we've been passed for this operation

  config match {
    case cfg: DistributedFilesystemWriteConfig =>
      cfg.createExternalTable match {
        case Some(value) =>
          if (value != "existing") {
            writer.openWrite() match {
              case Right(_) => ()
              case Left(err) => ErrorHandling.logAndThrowError(logger, err)
            }
          }
          else { () }

        case None =>
          writer.openWrite() match {
            case Right(_) => ()
            case Left(err) => ErrorHandling.logAndThrowError(logger, err)
          }
    }
  }


  /**
  * Writes the row to datasource. Not permanent until a commit from the driver happens
  *
  * @param record The row to be written to the source.
  */
  override def write(record: InternalRow): Unit = {
    writer.writeRow(record) match {
      case Right(_) => ()
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
    }
  }

/**
  * Initiates final stages of writing for a sucessful write of this partition. This does not act as a final commit as that will be done by [[VerticaBatchWrite.commit]] from the driver.
  *
  * @return org.apache.spark.sql.connector.write.WriterCommitMessage
  */
  override def commit(): WriterCommitMessage = {
    writer.closeWrite() match {
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
      case Right(_) => WriteSucceeded
    }
  }

/**
  * Initiates final stages of writing for a failed write of this partition.
  */
  override def abort(): Unit = {
    // Ignore the error here because the error that caused the abort is more relevant
    Utils.ignore(writer.closeWrite())
  }

/**
  * Called when all rows have been written.
  *
  * Calls any necessary cleanup. Called after commit or abort.
  */
  override def close(): Unit = {}
}
