package com.vertica.spark.util.version

import org.apache.spark.sql.SparkSession
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class SparkVersionTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  it should "get Spark version" in {
    val version = SparkVersionUtils.getVersion("3.2.1")
    assert(version.compare(SparkVersion(3,2,1)) == 0)
  }

  it should "get default Spark version on fail" in {
    val version = SparkVersionUtils.getVersion("abc")
    assert(version.compare(SparkVersionUtils.DEFAULT_SPARK) == 0)
  }

  it should "compare correctly Spark 3.2.0" in {
    assert(SparkVersionUtils.compareWith32(SparkVersion(3,2,0).toString) == 0)
    assert(SparkVersion(3,2,1).compare(SparkVersion(3,2,0)) == 1)
    assert(SparkVersion(3,2,1).compare(SparkVersion(3,2,2)) == -1)
  }
}
