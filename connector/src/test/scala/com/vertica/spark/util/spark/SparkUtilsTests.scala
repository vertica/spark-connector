package com.vertica.spark.util.spark

import com.vertica.spark.util.version.Version
import org.apache.spark.sql.SparkSession
import org.scalatest.flatspec.AnyFlatSpec

class SparkUtilsTests extends AnyFlatSpec {

  behavior of "SparkUtilsTests"

  it should "correctly parses Spark version string" in {
    val version = SparkUtils.getVersion(Some("3.2.1"))
    assert(version.isDefined)
    assert(version.get.isEquals(Version(3,2,1)))
  }

  it should "correctly parses major-minor-patch numbers" in {
    val version = SparkUtils.getVersion(Some("3.2.1-0-vertica-1"))
    assert(version.isDefined)
    assert(version.get.isEquals(Version(3,2,1)))
  }

  it should "return a Spark version string" in {
    val spark = SparkSession.builder().master("local[1]").getOrCreate()
    assert(SparkUtils.getVersionString.isDefined)
    spark.close()
  }
}
