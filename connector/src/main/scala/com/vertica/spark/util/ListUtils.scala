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

package com.vertica.spark.util

import cats.data.NonEmptyList
import cats.implicits._
import com.vertica.spark.util.error.{ConnectorError, ErrorList, SchemaError, SchemaErrorList}

object ListUtils {

  def listToEither[T](list: Seq[Either[ConnectorError, T]]): Either[ErrorList, Seq[T]] = {
    list.toList
      // converts List[Either[A, B]] to Either[List[A], List[B]]
      .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated)
      .toEither
      .map(field => field)
      .left.map(errors => ErrorList(errors))
  }

  def listToEitherSchema[T](list: Seq[Either[SchemaError, T]]): Either[SchemaErrorList, Seq[T]] = {
    list.toList
      // converts List[Either[A, B]] to Either[List[A], List[B]]
      .traverse(_.leftMap(err => NonEmptyList.one(err)).toValidated)
      .toEither
      .map(field => field)
      .left.map(errors => SchemaErrorList(errors))
  }
}
