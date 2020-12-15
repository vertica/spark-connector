package com.vertica.spark.datasource.v2

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.catalyst.InternalRow


/**
  * Builds the class for use in writing to Vertica
  */
class VerticaWriteBuilder extends WriteBuilder {
/**
  * Builds the class representing a write operation to a Vertica table
  *
  * @return [[VerticaBatchWrite]]
  */
  override def buildForBatch() : BatchWrite = {
    new VerticaBatchWrite()
  }
}

/**
  * Represents a write operation to Vertica
  *
  * Extends mixin class to represent type of write. Options are Batch or Stream, we are doing a batch write.
  */
class VerticaBatchWrite extends BatchWrite {

/**
  * Creates the writer factory which will be serialized and sent to workers
  *
  * @param physicalWriteInfo Structure containing partition information.
  * @return [[VerticaWriterFactory]]
  */
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new VerticaWriterFactory()

/**
  * Responsible for commiting the write operation.
  *
  * @param writerCommitMessages list of commit messages returned from each worker node
  * Called after all worker nodes report that they have succesfully completed their operations.
  */
  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

/**
  * Responsible for cleaning up a failed write operation.
  *
  * @param writerCommitMessages list of commit messages returned from each worker node
  * Called after one or more worker nodes report that they have failed.
  */
  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

/**
  * Factory class for creating the Vertica writer
  *
  * This class is seriazlized and sent to each worker node. On the worker, createWriter will be called with a given unique id for the partition being written.
  */
class VerticaWriterFactory extends DataWriterFactory {

/**
  * Called from the worker node to get the writer for that node
  *
  * @param partitionId A unique identifier for the partition being written
  * @param taskId A unique identifier for the specific task, which there may be multiple of for a partition due to retries or speculative execution
  * @return [[VerticaBatchWriter]]
  */
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new VerticaBatchWriter()
}

/**
  * Writer class that passes rows to be written to the underlying datasource
  */
class VerticaBatchWriter extends DataWriter[InternalRow] {
  object WriteSucceeded extends WriterCommitMessage

/**
  * Writes the row to datasource. Not permanent until a commit from the driver happens
  *
  * @param record The row to be written to the source.
  */
  override def write(record: InternalRow): Unit = {}

/**
  * Initiates final stages of writing for a sucessful write of this partition. This does not act as a final commit as that will be done by [[VerticaBatchWrite.commit]] from the driver.
  *
  * @return org.apache.spark.sql.connector.write.WriterCommitMessage
  */
  override def commit(): WriterCommitMessage = WriteSucceeded

/**
  * Initiates final stages of writing for a failed write of this partition.
  */
  override def abort(): Unit = {}

/**
  * Called when all rows have been written.
  *
  * Calls any necessary cleanup. Called after commit or abort.
  */
  override def close(): Unit = {}
}
