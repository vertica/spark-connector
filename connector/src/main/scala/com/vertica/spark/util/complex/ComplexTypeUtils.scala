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

package com.vertica.spark.util.complex

import org.apache.spark.sql.types.{ArrayType, MapType, StructField, StructType}

import scala.util.Either

class ComplexTypeUtils {

  def getComplexTypeColumns(schema: StructType):  (List[StructField], List[StructField]) = {
    val initialAccumulators: (List[StructField], List[StructField]) = (List(), List())
    schema
      .foldLeft(initialAccumulators)((acc, col) => {
        val (nativeCols, complexTypeCols) = acc
        if (isNativeType(col)) {
          (col :: nativeCols, complexTypeCols)
        } else {
          (nativeCols, col :: complexTypeCols)
        }
      })
  }

  /*
  * Check if field is a vertica native type. Vertica native types contains 1D arrays
  * */
  private def isNativeType(field: StructField): Boolean = {
    field.dataType match {
      case ArrayType(elementType, _) =>
        elementType match {
          case MapType(_, _, _) | StructType(_) | ArrayType(_, _) => false
          case _ => true
        }
      case MapType(_, _, _) | StructType(_) => false
      case _ => true
    }
  }
}
