package com.vertica.spark.datasource.v2

import org.apache.spark.sql.connector.write._
import org.apache.spark.sql.catalyst.InternalRow


class VerticaWriteBuilder extends WriteBuilder {
  override def buildForBatch() : BatchWrite =  new VerticaBatchWrite()
}

class VerticaBatchWrite extends BatchWrite {
  override def createBatchWriterFactory(physicalWriteInfo: PhysicalWriteInfo): DataWriterFactory = new VerticaWriterFactory()

  override def commit(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}

  override def abort(writerCommitMessages: Array[WriterCommitMessage]): Unit = {}
}

class VerticaWriterFactory extends DataWriterFactory {
  override def createWriter(partitionId: Int, taskId:Long): DataWriter[InternalRow] = new VerticaBatchWriter()
}

class VerticaBatchWriter extends DataWriter[InternalRow] {
  object WriteSucceeded extends WriterCommitMessage

  override def write(record: InternalRow): Unit = {}

  override def commit(): WriterCommitMessage = WriteSucceeded

  override def abort(): Unit = {}

  override def close(): Unit = {}
}
