package com.vertica.spark.util.compatibilities

import com.vertica.spark.config.ReadConfig
import com.vertica.spark.datasource.core.DSConfigSetupInterface
import com.vertica.spark.datasource.v2.{VerticaScanBuilder, VerticaScanBuilderWithPushdown}
import com.vertica.spark.util.reflections.ReflectionTools
import com.vertica.spark.util.version.Version
import org.scalamock.scalatest.MockFactory
import org.scalatest.flatspec.AnyFlatSpec

class DSTableCompatibilityToolsTest extends AnyFlatSpec with MockFactory{

  behavior of "DSTableCompatibilityUtilsTest"

  it should "build VerticaScanBuilder for Spark version before than 3.2" in {
    val sparkVersion = Version(3, 1, 9)
    val reflection = mock[ReflectionTools]
    (reflection.makeScanBuilderWithoutPushDown _).expects(*, *).returning(mock[VerticaScanBuilder])

    new DSTableCompatibilityTools(reflection).makeVerticaScanBuilder(sparkVersion, mock[ReadConfig], mock[DSConfigSetupInterface[ReadConfig]])
  }

  it should "build VerticaScanBuilder with aggregates push down for Spark version 3.2 or newer" in {
    val sparkVersion = Version(3, 2)
    val reflection = mock[ReflectionTools]
    (reflection.makeScanBuilderWithPushDown _).expects(*, *).returning(mock[VerticaScanBuilderWithPushdown])

    new DSTableCompatibilityTools(reflection).makeVerticaScanBuilder(sparkVersion, mock[ReadConfig], mock[DSConfigSetupInterface[ReadConfig]])
  }

}
