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

package com.vertica.spark.util.pushdown

import org.apache.spark.sql.sources._
import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

class PushdownUtilsTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {

  val sparkFilters: Array[Filter] = Array(
    EqualTo("a", 5),
    GreaterThan("a", 6.7),
    GreaterThanOrEqual("a", 8.8f),
    LessThan("a", 1000000),
    LessThanOrEqual("a", -1000000),
    In("a", Array("abc", "123", "456")),
    IsNull("a"),
    IsNotNull("b"),
    Not(EqualTo("a", 5)),
    StringStartsWith("a", "abc"),
    StringEndsWith("a", "qwe"),
    StringContains("a", "zxc")
                             )

  val textFilters: Array[String] = Array(
    "(\"a\" = 5)",
    "(\"a\" > 6.7)",
    "(\"a\" >= 8.8)",
    "(\"a\" < 1000000)",
    "(\"a\" <= -1000000)",
    "(\"a\" IN ('abc', '123', '456'))",
    "(\"a\" IS NULL)",
    "(\"b\" IS NOT NULL)",
    "( NOT (\"a\" = 5))",
    "(\"a\" like 'abc%')",
    "(\"a\" like '%qwe')",
    "(\"a\" like '%zxc%')"
  )

  it should "generate all filters" in {
    sparkFilters.indices.map( i => {
      val filter = sparkFilters(i)
      val text = textFilters(i)

      assert(PushdownUtils.genFilter(filter).right.get.filterString.toLowerCase == text.toLowerCase)
    })
  }

  it should "compose all filters with AND" in {
    sparkFilters.indices.map( i => {
      sparkFilters.indices.map( j => {
        val filter = And(sparkFilters(i), sparkFilters(j))
        val text = "(" + textFilters(i) + " AND " + textFilters(j) + ")"

        assert(PushdownUtils.genFilter(filter).right.get.filterString.toLowerCase == text.toLowerCase)
      })
    })
  }

  it should "compose all filters with OR" in {
    sparkFilters.indices.map( i => {
      sparkFilters.indices.map( j => {
        val filter = Or(sparkFilters(i), sparkFilters(j))
        val text = "(" + textFilters(i) + " OR " + textFilters(j) + ")"

        assert(PushdownUtils.genFilter(filter).right.get.filterString.toLowerCase == text.toLowerCase)
      })
    })
  }

  it should "compose all filters with AND + OR + NOT" in {
    sparkFilters.indices.map( i => {
      sparkFilters.indices.map( j => {
        val filter = Not(Or(And(sparkFilters(i), sparkFilters(j)), And(sparkFilters(i), sparkFilters(j))) )
        val text = "( NOT (" +
                   "(" + textFilters(i) + " AND " + textFilters(j) + ")" +
                   " OR " +
                   "(" + textFilters(i) + " AND " + textFilters(j) + ")" +
                  "))"

        assert(PushdownUtils.genFilter(filter).right.get.filterString.toLowerCase == text.toLowerCase)
      })
    })
  }
}
