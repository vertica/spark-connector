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

import com.typesafe.scalalogging.Logger
import com.vertica.spark.config.{DistributedFilesystemReadConfig, LogProvider, ReadConfig}
import com.vertica.spark.datasource.core.{DSConfigSetupInterface, DSReader, DSReaderInterface}
import com.vertica.spark.datasource.json.{JsonBatchFactory, VerticaJsonScan}
import com.vertica.spark.util.compatibilities.DSReadCompatibilityTools
import com.vertica.spark.util.error.{ConnectorError, ConnectorException, ErrorHandling, InitialSetupPartitioningError}
import com.vertica.spark.util.pushdown.PushdownUtils
import com.vertica.spark.util.schema.ComplexTypesSchemaTools
import com.vertica.spark.util.spark.SparkUtils
import com.vertica.spark.util.version.{SparkVersionUtils, Version}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.connector.expressions.aggregate._
import org.apache.spark.sql.connector.read._
import org.apache.spark.sql.sources.Filter
import org.apache.spark.sql.types._

trait PushdownFilter {
  def getFilterString: String
}

case class PushFilter(filter: Filter, filterString: String) extends PushdownFilter {
  def getFilterString: String = this.filterString
}

case class NonPushFilter(filter: Filter) extends AnyVal

case class ExpectedRowDidNotExistError() extends ConnectorError {
  def getFullContext: String = "Fatal error: expected row did not exist"
}

case class AggregateNotSupported(aggregate: String) extends ConnectorError {
  def getFullContext: String = s"$aggregate is not supported"
}

case class UnknownColumnName(colName: String) extends ConnectorError {
  def getFullContext: String = s"$colName could not be found"
}

/**
  * Builds the scan class for use in reading of Vertica
  */
class VerticaScanBuilder(config: ReadConfig, readConfigSetup: DSConfigSetupInterface[ReadConfig]) extends ScanBuilder with
  SupportsPushDownFilters  with SupportsPushDownRequiredColumns {
  protected var pushFilters: List[PushFilter] = Nil

  protected var requiredSchema: StructType = StructType(Nil)

  protected var aggPushedDown: Boolean = false

  protected var groupBy: Array[StructField] = Array()

  protected val logger = LogProvider.getLogger(classOf[VerticaScanBuilder])

  protected val ctTools: ComplexTypesSchemaTools = new ComplexTypesSchemaTools()

  protected val compatibility = new DSReadCompatibilityTools

/**
  * Builds the class representing a scan of a Vertica table
  *
  * @return [[VerticaScan]]
  */
  override def build(): Scan = {
    val cfg = config.copyConfig()
    cfg.setPushdownFilters(this.pushFilters)
    cfg.setRequiredSchema(this.requiredSchema)
    cfg.setPushdownAgg(this.aggPushedDown)
    cfg.setGroupBy(this.groupBy)

    if(useJson(cfg)) {
      new VerticaJsonScan(cfg, readConfigSetup, new JsonBatchFactory)
    } else {
      new VerticaScan(cfg, readConfigSetup)
    }
  }

  private def useJson(cfg: ReadConfig): Boolean = {
    cfg match {
      case config: DistributedFilesystemReadConfig =>
        (readConfigSetup.getTableSchema(config), config.getRequiredSchema) match {
          case (Right(metadataSchema), requiredSchema) =>
            val schema: StructType = if (requiredSchema.nonEmpty) {
              requiredSchema
            } else {
              metadataSchema
            }
            config.useJson || ctTools.filterComplexTypeColumns(schema).nonEmpty
          case (Left(err), _) => ErrorHandling.logAndThrowError(logger, err)
        }
      case _=> false
    }
  }


  override def pushFilters(filters: Array[Filter]): Array[Filter] = {
    val initialLists: (List[NonPushFilter], List[PushFilter]) = (List(), List())
    val (nonPushFilters, pushFilters): (List[NonPushFilter], List[PushFilter]) = filters
      .map(PushdownUtils.genFilter)
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

  override def pruneColumns(requiredSchema: StructType): Unit = {
    this.requiredSchema = requiredSchema
    this.aggPushedDown = false
    this.groupBy = Array()
  }

  protected def getColType(colName: String): DataType = {
    tableSchema.find(_.name.equalsIgnoreCase(colName)) match {
      case Some(col) => col.dataType
      case None => ErrorHandling.logAndThrowError(logger, UnknownColumnName(colName))
    }
  }

  protected def tableSchema: StructType = readConfigSetup.getTableSchema(config) match {
    case Right(schema) => schema
    case Left(err) => ErrorHandling.logAndThrowError(logger, err.context("Scan builder failed to get table schema"))
  }
}

class VerticaScanBuilderWithPushdown(config: ReadConfig, readConfigSetup: DSConfigSetupInterface[ReadConfig]) extends VerticaScanBuilder(config, readConfigSetup) with SupportsPushDownAggregates {

  override def pushAggregation(aggregation: Aggregation): Boolean = {
    try{
      val aggregatesStructFields: Array[StructField] = aggregation.aggregateExpressions().map {
        case _: CountStar => StructField("COUNT(*)", LongType, nullable = false, Metadata.empty)
        case aggregate: Count => StructField(aggregate.describe(), LongType, nullable = false, Metadata.empty)
        case aggregate: Sum => StructField(aggregate.describe(), getColType(aggregate.column().describe()), nullable = false, Metadata.empty)
        case aggregate: Min => StructField(aggregate.describe(), getColType(aggregate.column().describe()), nullable = false, Metadata.empty)
        case aggregate: Max => StructField(aggregate.describe(), getColType(aggregate.column().describe()), nullable = false, Metadata.empty)
        // short circuit
        case aggregate => ErrorHandling.logAndThrowError(logger, AggregateNotSupported(aggregate.describe()))
      }
      val groupByColumnsStructFields = getGroupByColumns(aggregation)
      this.requiredSchema = StructType(groupByColumnsStructFields ++ aggregatesStructFields)
      this.groupBy = groupByColumnsStructFields
      this.aggPushedDown = true
    } catch{
      case e: ConnectorException => e.error match{
        // This instance of builder may be reused, so we reset.
        case _: AggregateNotSupported =>
          this.requiredSchema = StructType(Nil)
          this.groupBy = Array()
          this.aggPushedDown = false
        case _ => throw e
      }
      case e: Exception => throw e
    }
    // if false, the read is continued with no push down.
    this.aggPushedDown
  }

  private def getGroupByColumns(aggregation: Aggregation): Array[StructField] = {
    val sparkVersion = SparkVersionUtils.getVersion(SparkUtils.getVersionString).getOrElse(Version(3,3))
    compatibility
      .getGroupByExpressions(sparkVersion, aggregation)
      .map(expr => StructField(expr.describe, getColType(expr.describe), nullable = false, Metadata.empty))
  }
}

/**
  * Represents a scan of a Vertica table.
  *
  * Extends mixin class to represent type of read. Options are Batch or Stream, we are doing a batch read.
  */
class VerticaScan(config: ReadConfig, readConfigSetup: DSConfigSetupInterface[ReadConfig]) extends Scan with Batch {

  private val logger: Logger = LogProvider.getLogger(classOf[VerticaScan])

  def getConfig: ReadConfig = config

  /**
  * Schema of scan (can be different than full table schema)
  */
  override def readSchema(): StructType = {
    (readConfigSetup.getTableSchema(config), config.getRequiredSchema) match {
      case (Right(schema), requiredSchema) => if (requiredSchema.nonEmpty) { requiredSchema } else { schema }
      case (Left(err), _) => ErrorHandling.logAndThrowError(logger, err)
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
   readConfigSetup
      .performInitialSetup(config) match {
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
      case Right(opt) => opt match {
        case None => ErrorHandling.logAndThrowError(logger, InitialSetupPartitioningError())
        case Some(partitionInfo) => partitionInfo.partitionSeq
      }
    }
  }


/**
  * Creates the reader factory which will be serialized and sent to workers
  *
  * @return [[VerticaReaderFactory]]
  */
  override def createReaderFactory(): PartitionReaderFactory = {
    new VerticaReaderFactory(config)
  }
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
    new VerticaBatchReader(config, new DSReader(config, partition))
  }

}

/**
  * Reader class that reads rows from the underlying datasource
  */
class VerticaBatchReader(config: ReadConfig, reader: DSReaderInterface) extends PartitionReader[InternalRow] {
  private val logger: Logger = LogProvider.getLogger(classOf[VerticaBatchReader])

  // Open the read
  reader.openRead() match {
    case Right(_) => ()
    case Left(err) => ErrorHandling.logAndThrowError(logger, err)
  }

  var row: Option[InternalRow] = None

/**
  * Returns true if there are more rows to read
  */
  override def next: Boolean =
  {
    reader.readRow() match {
      case Left(err) => ErrorHandling.logAndThrowError(logger, err)
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
      case None => ErrorHandling.logAndThrowError(logger, ExpectedRowDidNotExistError())
      case Some(v) => v
    }
  }

/**
  * Calls underlying datasource to do any needed cleanup
  */
  def close(): Unit = {
    reader.closeRead() match {
      case Right(_) => ()
      case Left(e) => ErrorHandling.logAndThrowError(logger, e)
    }
  }
}
