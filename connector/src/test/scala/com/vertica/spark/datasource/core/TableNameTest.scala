package com.vertica.spark.datasource.core

import com.vertica.spark.config.TableName
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class TableNameTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory {
  it should "Escape table name" in {
    val tablename = TableName("t\"nam\"e", Some("Sch \" ema"))

    assert(tablename.getFullTableName == "\"Sch \"\" ema\".\"t\"\"nam\"\"e\"")
  }
}
