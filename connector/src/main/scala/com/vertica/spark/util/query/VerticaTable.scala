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
import com.vertica.spark.util.error.{ConnectorError, ErrorList, IntrospectionResultEmpty, MultipleIntrospectionResult, ResultSetError}

/**
 * Abstract class containing base implementation for querying data from Vertica tables. Subclasses are expected to
 * implement public methods that provides queries to a table. They will also need to specify a type representing
 * a row of data.
 *
 * @param jdbc A jdbc layer used to query to Vertica
 * @tparam T A concrete type representing a row of data from the table.
 * */
abstract class VerticaTable[T](jdbc: JdbcLayerInterface) {

  /**
   * @return The value to be used in the FROM clause of the query.
   * */
  protected def tableName: String

  /**
   * @return A list of columns to used in the SELECT clause fo the query
   * */
  protected def columns: Seq[String]

  /**
   * When a query contains some data, this function is called to construct row data object until
   * the result set iterator is false.
   *
   * Try catch block are not needed as exceptions are caught above this function.
   *
   * @param rs a result set containing the next row of data.
   * @return the object representing a row data.
   * */
  protected def buildRow(rs: ResultSet): T

  /**
   * Make a query using parameters from concrete classes.
   * */
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

  /**
   * Available for sub classes to make a query expecting some results.
   *
   * @param conditions the predicates string to be used in the WHERE clause of the query
   * @return Either a list of row data or error
   * */
  protected final def selectWhere(conditions: String): ConnectorResult[Seq[T]] = {
    _selectWhere(conditions)._1
  }

  /**
   * Available for sub classes to make a query expecting only one result. Will error otherwise.
   *
   * @param conditions the predicates string to be used in the WHERE clause of the query
   * @return Either a single row data or error
   * */
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