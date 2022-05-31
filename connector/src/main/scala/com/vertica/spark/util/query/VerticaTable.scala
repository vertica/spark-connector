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

package com.vertica.spark.util.query

import com.vertica.spark.datasource.jdbc.JdbcLayerInterface
import com.vertica.spark.util.error.ErrorHandling.ConnectorResult

import java.sql.ResultSet
import scala.util.Try
import cats.implicits._
import cats.data.NonEmptyList
import com.vertica.spark.util.error.{ConnectorError, ErrorList, ResultSetError, SchemaError, VerticaColumnNotFound}

abstract class VerticaTable[T](jdbc: JdbcLayerInterface) {

  protected def tableName: String

  protected def columns: Seq[String]

  protected def buildRow(rs: ResultSet): T

  private final def _selectWhere(conditions: String): (ConnectorResult[Seq[T]], String) = {
    val cols = columns.map(wrapQuotation).mkString(", ")
    val tableName = wrapQuotation(this.tableName)
    val where = if (conditions.isEmpty) "" else " WHERE " + conditions.trim
    val query = s"SELECT $cols FROM $tableName$where"
    val result = jdbc.query(query) match {
      case Left(err) => Left(err)
      case Right(rs) =>
        var rowsOrErrors = List[Either[Throwable, T]]()
        while (rs.next) rowsOrErrors = rowsOrErrors :+ Try{buildRow(rs)}.toEither
        rs.close()
        rowsOrErrors
          .traverse(_.leftMap(err => NonEmptyList.one(ResultSetError(err))).toValidated)
          .toEither
          .map(rows => rows)
          .left.map(error => ErrorList(error))
    }
    (result, query)
  }

  protected final def selectWhere(conditions: String): ConnectorResult[Seq[T]] = {
    _selectWhere(conditions)._1
  }

  protected final def selectWhereExpectOne(conditions: String): Either[ConnectorError, T] = {
    val (result, query) = _selectWhere(conditions)
     result match {
      case Left(error) => Left(error)
      case Right(value) =>
        if(value.isEmpty)
          Left(IntrospectionResultEmpty(this.tableName, query))
        else if (value.length > 1)
          Left(MultipleIntrospectionResult(this.tableName, query))
        else
          Right(value.head)
    }
  }

  private def wrapQuotation(str: String): String = "\"" + str +"\""

}

sealed trait TableIntrospectionError extends ConnectorError

case class IntrospectionResultEmpty(table: String, query: String) extends TableIntrospectionError {
  override def getFullContext: String = s"Query to system table $table returned nothing.\nQUERY: $query"
}

case class MultipleIntrospectionResult(table: String, query: String) extends TableIntrospectionError {
  override def getFullContext: String = s"Query to system table $table return more than one result.\nQUERY: $query"
}