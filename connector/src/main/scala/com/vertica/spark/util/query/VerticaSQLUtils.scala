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

import scala.annotation.tailrec

/**
 * Class contains helper methods for parsing Vertica SQL queries. Ideally we would like a more robust parser however it
 * was not justified because:
 * 1. We have not needed to do a lot of SQL parsing, yet!
 * 2. We did not find an appropriate library for use
 * Should we start handling more SQL parsing, we will need to implement a custom parser.
 * */
object VerticaSQLUtils {

  /**
   * Return the indices of the first open parenthesis and its matching closing parenthesis
   * @return a tuple of (openParenIndex, closingParenIndex)
   * */
  def findFirstParenGroupIndices(str: String): (Int, Int) = {
    val openParenIndex = str.indexOf("(")
    val subString = str.substring(openParenIndex + 1)

    /**
     * This recursion finds the matching paren by tracking the paren count.
     * When it is 0 then we have the matching paren.
     * */
    @tailrec
    def findMatchingClosingParen(char: Char, tail: String, parenCount: Int): Int = {
      char match {
        case '(' => findMatchingClosingParen(tail.head, tail.tail, parenCount + 1)
        case ')' =>
          if (parenCount == 1) {
            subString.length - tail.length
          } else {
            findMatchingClosingParen(tail.head, tail.tail, parenCount - 1)
          }
        case _ => findMatchingClosingParen(tail.head, tail.tail, parenCount)
      }
    }

    val closingParenIndex = openParenIndex + findMatchingClosingParen(subString.head, subString.tail, 1)
    (openParenIndex, closingParenIndex)
  }

  /**
   * Split a string by comma. Will not split on a comma if it is between parentheses.
   * */
  def splitByComma(str: String): Seq[String] = {

    @tailrec
    def recursion(char: Char, tail: String, currStr: String = "", splits: List[String] = List(), parenCount: Int = 0): List[String] = {
      val posParenCount = if(parenCount < 0) 0 else parenCount
      val nextStr = currStr :+ char
      char match {
        // Keeping track of parenthesis to know if it should split or not
        case '(' if tail.nonEmpty => recursion(tail.head, tail.tail, nextStr, splits, posParenCount + 1)
        case ')' if tail.nonEmpty => recursion(tail.head, tail.tail, nextStr, splits, posParenCount - 1)
        case ',' if tail.nonEmpty =>
         if (posParenCount > 0) {
            recursion(tail.head, tail.tail, nextStr, splits, posParenCount)
          } else {
            recursion(tail.head, tail.tail, "", splits :+ currStr.trim, posParenCount)
          }
        case _ =>
          if (tail.isEmpty) {
            char match {
              case ',' if posParenCount == 0 => splits :+ currStr.trim
              case _ => splits :+ nextStr.trim
            }
          } else {
            recursion(tail.head, tail.tail, nextStr, splits, posParenCount)
          }
      }
    }

    recursion(str.head, str.tail)
  }
}
