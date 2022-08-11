package com.vertica.spark.util.query

import org.scalatest.flatspec.AnyFlatSpec

class VerticaSQLUtilsTest extends AnyFlatSpec {

  behavior of "VerticaSQLUtilsTest"

  it should "split a comma separated list" in {
    val result = VerticaSQLUtils.splitByComma("cat, cat dog, shark,")
    assert(result.length == 3)
    assert(result(0) == "cat")
    assert(result(1) == "cat dog")
    assert(result(2) == "shark")
  }

  it should "split a comma separated list with parentheses" in {
    val result = VerticaSQLUtils.splitByComma("col1 (int, col2) (cat, ((dog))), shark")
    assert(result.length == 2)
    assert(result.head == "col1 (int, col2) (cat, ((dog)))")
    assert(result(1) == "shark")

    val result2 = VerticaSQLUtils.splitByComma("(col1 int, col2 cat dog,)")
    assert(result2.length == 1)
    assert(result2.head == "(col1 int, col2 cat dog,)")

    val result3 = VerticaSQLUtils.splitByComma(")(col1 (int, ()col2, shark)), cat, dog")
    assert(result3.length == 3)
    assert(result3.head == ")(col1 (int, ()col2, shark))")
    assert(result3(1) == "cat")
    assert(result3(2) == "dog")
  }

  it should "split a comma separated list with non matching parentheses" in {
    val result = VerticaSQLUtils.splitByComma("(col1 int, col2 cat dog,")
    assert(result.length == 1)
    assert(result.head == "(col1 int, col2 cat dog,")

    val result2 = VerticaSQLUtils.splitByComma(")col1 (int, (col2, shark)), cat, dog")
    assert(result2.length == 3)
    assert(result2.head == ")col1 (int, (col2, shark))")
    assert(result2(1) == "cat")
    assert(result2(2) == "dog")

    val result3 = VerticaSQLUtils.splitByComma(")(col1 (int, (col2, shark)), cat, dog")
    assert(result3.length == 1)
    assert(result3.head == ")(col1 (int, (col2, shark)), cat, dog")
  }

  it should "find the indices of the first matching parentheses" in {
    val str = ")cat(dog_(sha(rk)))cat(_(d)og)"
    val (openParen, closeParen) = VerticaSQLUtils.findFirstParenGroupIndices(str)
    assert(openParen == 4)
    assert(closeParen == 18)
    assert(str.substring(openParen + 1, closeParen) == "dog_(sha(rk))")
  }

}
