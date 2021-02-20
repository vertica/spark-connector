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

import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.InternalRow
import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.{DSReadConfigSetup, DSReader, VerticaPipeFactoryWithFilters}
import com.vertica.spark.util.error.ConnectorError
import com.vertica.spark.util.error.ConnectorErrorType.PartitioningError
import org.apache.spark.sql.sources.{
  And,
  EqualTo,
  Filter,
  GreaterThan,
  GreaterThanOrEqual,
  In,
  IsNotNull,
  IsNull,
  LessThan,
  LessThanOrEqual,
  Not,
  Or,
  StringContains,
  StringEndsWith,
  StringStartsWith
}

trait PushdownFilter {
  def getFilterString: String
}

case class PushFilter(filter: Filter, filterString: String) extends PushdownFilter {
  def getFilterString: String = this.filterString
}

case class NonPushFilter(filter: Filter) extends AnyVal

/**
  * Builds the scan class for use in reading of Vertica
  */
class VerticaScanBuilder(config: ReadConfig) extends ScanBuilder with SupportsPushDownFilters {
  private var pushFilters: List[PushFilter] = Nil

/**
  * Builds the class representing a scan of a Vertica table
  *
  * @return [[VerticaScan]]
  */
  override def build(): Scan = {
    new VerticaScan(config, this.pushFilters)
  }

  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val initialLists: (List[NonPushFilter], List[PushFilter]) = (List(), List())
    val (nonPushFilters, pushFilters): (List[NonPushFilter], List[PushFilter]) = filters
      .map(genFilterStr)
      .foldLeft(initialLists)((acc, filter) => {
        val (nonPushFilters, pushFilters) = acc
        filter match {
          case Left(nonPushFilter) => (nonPushFilter :: nonPushFilters, pushFilters)
          case Right(pushFilter) => (nonPushFilters, pushFilter :: pushFilters)
        }
      })

    this.pushFilters = pushFilters

    nonPushFilters.map(_.filter).toArray
  }

  override def pushedFilters(): Array[Filter] = {
    this.pushFilters.map(_.filter).toArray
  }

  private def wrapText(value: Any): String = {
    value match {
      case d: java.lang.Number =>
        if (d.doubleValue.isInfinite) {
          "'" + value.toString + "'"
        } else {
          value.toString
        }
      case _ => "'" + value.toString + "'"
    }
  }

  def genFilterStr(filter: Filter): Either[NonPushFilter, PushFilter] = {
    filter match {
      case EqualTo(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " = " + wrapText(value) + ")"))
      case GreaterThan(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\""  + " > " + wrapText(value) + ")"))
      case GreaterThanOrEqual(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " >= " + wrapText(value) + ")"))
      case LessThan(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\""  + " < " + wrapText(value) + ")"))
      case LessThanOrEqual(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\""  + " <= " + wrapText(value) + ")"))
      case In(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\""  + " in " +
        "(" + value.map(x => wrapText(x)).mkString(", ") + ")" + ")"))
      case IsNull(attribute) => Right(PushFilter(filter, "(" + "\"" + attribute + "\""  + " is NULL" + ")"))
      case IsNotNull(attribute) => Right(PushFilter(filter, "(" + "\"" + attribute + "\""  + " is NOT NULL" + ")"))
      case And(left, right) => (for {
        pushFilterLeft <- genFilterStr(left)
        pushFilterRight <- genFilterStr(right)
      } yield PushFilter(filter, "(" + pushFilterLeft.filterString + " AND " + pushFilterRight.filterString + ")"))
        .left.map(_ => NonPushFilter(filter))
      case Or(left, right) => (for {
        pushFilterLeft <- genFilterStr(left)
        pushFilterRight <- genFilterStr(right)
      } yield PushFilter(filter, "(" + pushFilterLeft.filterString + " OR " + pushFilterRight.filterString + ")"))
        .left.map(_ => NonPushFilter(filter))
      case Not(child) => genFilterStr(child) match {
        case Left(_) => Left(NonPushFilter(filter))
        case Right(pushFilter) => Right(PushFilter(filter, "(" + " NOT " + pushFilter.filterString + ")"))
      }
      case StringStartsWith(attribute, value) => Right(PushFilter(filter, "(" +  "\"" + attribute + "\"" +
        " like " + wrapText(value + "%") + ")"))
      case StringEndsWith(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\""  +
        " like " + wrapText("%" + value) + ")"))
      case StringContains(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\""  +
        " like " + wrapText("%" + value + "%") + ")"))
      case _ => Left(NonPushFilter(filter))
    }
  }
}


/**
  * Represents a scan of a Vertica table.
  *
  * Extends mixin class to represent type of read. Options are Batch or Stream, we are doing a batch read.
  */
class VerticaScan(config: ReadConfig, pushdownFilters: List[PushdownFilter]) extends Scan with Batch {
  /**
  * Schema of scan (can be different than full table schema)
  */
  override def readSchema(): StructType = {
    (new DSReadConfigSetup).getTableSchema(config) match {
      case Right(schema) => schema
      case Left(err) => throw new Exception(err.msg)
    }
  }

/**
  * Returns this object as an instance of the Batch interface
  */
  override def toBatch: Batch = this


/**
  * Returns an array of partitions. These contain the information necesary for each reader to read it's portion of the data
  */
  override def planInputPartitions(): Array[InputPartition] = {
    new DSReadConfigSetup(pipeFactory = VerticaPipeFactoryWithFilters(this.pushdownFilters))
      .performInitialSetup(config) match {
      case Left(err) => throw new Exception(err.msg)
      case Right(opt) => opt match {
        case None =>
          val err = ConnectorError(PartitioningError)
          throw new Exception(err.msg)
        case Some(partitionInfo) => partitionInfo.partitionSeq
      }
    }
  }


/**
  * Creates the reader factory which will be serialized and sent to workers
  *
  * @return [[VerticaReaderFactory]]
  */
  override def createReaderFactory(): PartitionReaderFactory = new VerticaReaderFactory(config)
}

/**
  * Factory class for creating the Vertica reader
  *
  * This class is seriazlized and sent to each worker node. On the worker, createReader will be called with the given partition of data for that worker.
  */
class VerticaReaderFactory(config: ReadConfig) extends PartitionReaderFactory {
/**
  * Called from the worker node to get the reader for that node
  *
  * @return [[VerticaBatchReader]]
  */
  override def createReader(partition: InputPartition): PartitionReader[InternalRow] =
  {
    new VerticaBatchReader(config, partition)
  }

}

/**
  * Reader class that reads rows from the underlying datasource
  */
class VerticaBatchReader(config: ReadConfig, partition: InputPartition) extends PartitionReader[InternalRow] {

  val reader = new DSReader(config, partition)

  // Open the read
  reader.openRead()

  var row: Option[InternalRow] = None

/**
  * Returns true if there are more rows to read
  */
  override def next: Boolean =
  {
    reader.readRow() match {
      case Left(err) =>
        throw new Exception(err.msg)
      case Right(r) =>
        row = r
    }
    row match {
      case Some(_) => true
      case None => false
    }
  }

/**
  * Return the current row
  */
  override def get: InternalRow = {
    row match {
      case None => throw new Exception("Fatal error: expected row did not exist")
      case Some(v) => v
    }
  }

/**
  * Calls underlying datasource to do any needed cleanup
  */
  def close(): Unit = reader.closeRead()
}
