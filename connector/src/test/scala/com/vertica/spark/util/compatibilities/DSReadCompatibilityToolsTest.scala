package com.vertica.spark.util.compatibilities

import com.vertica.spark.util.reflections.ReflectionTools
import com.vertica.spark.util.version.Version
import org.apache.spark.sql.connector.expressions.aggregate.{AggregateFunc, Aggregation}
import org.apache.spark.sql.connector.expressions.Expression
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class DSReadCompatibilityToolsTest extends AnyFlatSpec with MockFactory {

  behavior of "DSReadCompatibilityToolsTest"

  private val groupByExpressions = Array[Expression]()
  private val aggregates = Array[AggregateFunc]()
  // Aggregation is final and can't be mocked with MockFactory
  private val mockAggregation = new Aggregation(aggregates, groupByExpressions)

  it should "get group by expressions from groupByColumns method when Spark is than 3.2.x" in {
    val sparkVersion = Version(3,2,9)
    val reflection = mock[ReflectionTools]
    val groupByColumns = Array[Expression]()
    (reflection.aggregationInvokeMethod[Array[Expression]] _).expects(mockAggregation, "groupByColumns").returning(groupByColumns)

    assert(new DSReadCompatibilityTools(reflection).getGroupByExpressions(sparkVersion, mockAggregation) == groupByColumns)
  }

  it should "get group by expressions using groupByExpressions method when spark is at least 3.3.0" in {
    val sparkVersion = Version(3,3)
    assert(new DSReadCompatibilityTools(mock[ReflectionTools]).getGroupByExpressions(sparkVersion, mockAggregation) == groupByExpressions)
  }

}
