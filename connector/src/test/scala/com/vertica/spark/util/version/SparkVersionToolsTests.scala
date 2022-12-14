package com.vertica.spark.util.version

import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.v2.{VerticaScanBuilder, VerticaScanBuilderWithPushdown}
import com.vertica.spark.util.reflections.ReflectionTools
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connector.expressions.Expression
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation}
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class SparkVersionToolsTests extends AnyFlatSpec with MockFactory{

  behavior of "SparkUtilsTests"

  it should "correctly parses Spark version string" in {
    val version = (new SparkVersionTools).getVersion(Some("3.2.1"))
    assert(version.isDefined)
    assert(version == Some(Version(3,2,1)))
  }

  it should "correctly parses major-minor-patch numbers" in {
    val version = (new SparkVersionTools).getVersion(Some("3.2.1-0-vertica-1"))
    assert(version.isDefined)
    assert(version == Some(Version(3,2,1)))
  }

  it should "return a Spark version string" in {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    assert((new SparkVersionTools).getVersionString.isDefined)
    spark.close()
  }

  private val groupByExpressions = Array[Expression]()
  private val aggregates = Array[AggregateFunc]()
  // Aggregation is final and can't be mocked with MockFactory
  private val mockAggregation = new Aggregation(aggregates, groupByExpressions)

  it should "get group by expressions from groupByColumns method when Spark is than 3.2.x" in {
    val sparkVersion = Version(3,2,9)
    val reflection = mock[ReflectionTools]
    val groupByColumns = Array[Expression]()
    (reflection.aggregationInvokeMethod[Array[Expression]] _).expects(mockAggregation, "groupByColumns").returning(groupByColumns)

    assert(new SparkVersionTools(reflection).getCompatibleGroupByExpressions(sparkVersion, mockAggregation) == groupByColumns)
  }

  it should "get group by expressions using groupByExpressions method when spark is at least 3.3.0" in {
    val sparkVersion = Version(3,3)
    assert(new SparkVersionTools(mock[ReflectionTools]).getCompatibleGroupByExpressions(sparkVersion, mockAggregation) == groupByExpressions)
  }

  it should "build VerticaScanBuilder for Spark version before than 3.2" in {
    val sparkVersion = Version(3, 1, 9)
    val reflection = mock[ReflectionTools]
    (reflection.makeScanBuilderWithoutPushDown _).expects(*, *).returning(mock[VerticaScanBuilder])

    new SparkVersionTools(reflection).makeCompatibleVerticaScanBuilder(sparkVersion, mock[ReadConfig], mock[DSConfigSetupInterface[ReadConfig]])
  }

  it should "build VerticaScanBuilder with aggregates push down for Spark version 3.2 or newer" in {
    val sparkVersion = Version(3, 2)
    val reflection = mock[ReflectionTools]
    (reflection.makeScanBuilderWithPushDown _).expects(*, *).returning(mock[VerticaScanBuilderWithPushdown])

    new SparkVersionTools(reflection).makeCompatibleVerticaScanBuilder(sparkVersion, mock[ReadConfig], mock[DSConfigSetupInterface[ReadConfig]])
  }
}
