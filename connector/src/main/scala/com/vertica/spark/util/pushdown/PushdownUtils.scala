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

import com.vertica.spark.datasource.v2.{NonPushFilter, PushFilter}
import org.apache.spark.sql.sources._

/**
 * Utils class for translating Spark pushdown filters into Vertica-compatible SQL where clauses.
 */
object PushdownUtils {
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

  /**
   * Takes a filter, and if we support it, turns it into a format we can add to our queries, therefore pushing down said filter.
   *
   * @param filter Spark-defined filter class.
   * @return A PushFilter containing a string to add to a select from Vertica, or a NonPushFilter if we don't support that filter.
   */
  def genFilter(filter: Filter): Either[NonPushFilter, PushFilter] = {
    filter match {
      case EqualTo(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " = " + wrapText(value) + ")"))
      case GreaterThan(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " > " + wrapText(value) + ")"))
      case GreaterThanOrEqual(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " >= " + wrapText(value) + ")"))
      case LessThan(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " < " + wrapText(value) + ")"))
      case LessThanOrEqual(attribute, value) => Right(PushFilter(filter,
        "(" + "\"" + attribute + "\"" + " <= " + wrapText(value) + ")"))
      case In(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" + " in " +
        "(" + value.map(x => wrapText(x)).mkString(", ") + ")" + ")"))
      case IsNull(attribute) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" + " is NULL" + ")"))
      case IsNotNull(attribute) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" + " is NOT NULL" + ")"))
      case And(left, right) => (for {
        pushFilterLeft <- genFilter(left)
        pushFilterRight <- genFilter(right)
      } yield PushFilter(filter, "(" + pushFilterLeft.filterString + " AND " + pushFilterRight.filterString + ")"))
        .left.map(_ => NonPushFilter(filter))
      case Or(left, right) => (for {
        pushFilterLeft <- genFilter(left)
        pushFilterRight <- genFilter(right)
      } yield PushFilter(filter, "(" + pushFilterLeft.filterString + " OR " + pushFilterRight.filterString + ")"))
        .left.map(_ => NonPushFilter(filter))
      case Not(child) => genFilter(child) match {
        case Left(_) => Left(NonPushFilter(filter))
        case Right(pushFilter) => Right(PushFilter(filter, "(" + " NOT " + pushFilter.filterString + ")"))
      }
      case StringStartsWith(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" +
        " like " + wrapText(value + "%") + ")"))
      case StringEndsWith(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" +
        " like " + wrapText("%" + value) + ")"))
      case StringContains(attribute, value) => Right(PushFilter(filter, "(" + "\"" + attribute + "\"" +
        " like " + wrapText("%" + value + "%") + ")"))
      case _ => Left(NonPushFilter(filter))
    }
  }
}
