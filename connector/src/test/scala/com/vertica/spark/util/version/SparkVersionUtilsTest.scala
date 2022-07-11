package com.vertica.spark.util.version

import org.scalatest.flatspec.AnyFlatSpec

class SparkVersionUtilsTest extends AnyFlatSpec {

  behavior of "SparkVersionUtilsTest"

  it should "correctly parses Spark version string" in {
    val version = SparkVersionUtils.getVersion(Some("3.2.1"))
    assert(version.isDefined)
    assert(version.get.isEquals(Version(3,2,1)))
  }

  it should "correctly parses major-minor-patch numbers" in {
    val version = SparkVersionUtils.getVersion(Some("3.2.1-0-vertica-1"))
    assert(version.isDefined)
    assert(version.get.isEquals(Version(3,2,1)))
  }
}
