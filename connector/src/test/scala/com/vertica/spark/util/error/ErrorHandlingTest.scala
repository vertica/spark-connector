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

package com.vertica.spark.util.error

import org.scalamock.scalatest.MockFactory
import org.scalatest.BeforeAndAfterAll
import org.scalatest.flatspec.AnyFlatSpec

trait TestError extends ConnectorError


case class MyError() extends TestError {
  private val message = "My test error"

  def getFullContext: String = this.message
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}

case class InterceptError(error: ConnectorError) extends TestError {
  private val message = "Intercept test error"

  def getFullContext: String = ErrorHandling.appendErrors(this.message, this.error.getFullContext)
  def getError: ConnectorError = this
  def getUserMessage: String = this.message
}

class ErrorHandlingTest extends AnyFlatSpec with BeforeAndAfterAll with MockFactory with org.scalatest.OneInstancePerTest {
  def addContextA(): ConnectorError = {
    addContextB().context("Failure when calling addContextA")
  }

  def addContextB(): ConnectorError = {
    myError().context("Failure when calling addContextB")
  }

  def myError(): ConnectorError = {
    MyError()
  }

  def interceptError(): ConnectorError = {
    InterceptError(addContextA())
  }

  def addContextC(): ConnectorError = {
    interceptError().context("Failure when calling addContextC")
  }

  it should "gather context for the returned error" in {
    assert(addContextA().getFullContext ==
      "Failure when calling addContextA\n" +
      "Failure when calling addContextB\n" +
      "My test error"
    )
  }

  it should "only print the user friendly error" in {
    assert(addContextA().getUserMessage == "My test error")
  }

  it should "allow downcasting to the TestError type" in {
    assert(addContextA().getError match {
      case _: TestError => true
      case _ => false
    })
  }

  it should "allow downcasting to the more specific MyError type" in {
    assert(addContextA().getError match {
      case MyError() => true
      case _ => false
    })
  }

  it should "catch a ConnectorException and allow downcasting to the specific error type" in {
    try {
      throw new ConnectorException(addContextA())
    } catch {
      case e: ConnectorException => assert(e.error.getError match {
        case MyError() => true
        case _ => false
      })
      case _: Exception => fail("The wrong exception occurred.")
    }
  }

  it should "catch a ConnectorException and get the underlying error's message" in {
    try {
      throw new ConnectorException(addContextA())
    } catch {
      case e: ConnectorException => assert(e.getMessage == "My test error")
      case _: Exception => fail("The wrong exception occurred.")
    }
  }

  it should "properly combine the intercepted error message and context with the new error" in {
    assert(addContextC().getFullContext ==
      "Failure when calling addContextC\n" +
      "Intercept test error\n" +
      "Failure when calling addContextA\n" +
      "Failure when calling addContextB\n" +
      "My test error"
    )
  }

  it should "replace the old error's user friendly error message with the new error's user friendly message" in {
    assert(addContextC().getUserMessage == "Intercept test error")
  }

  it should "still allow accessing the underlying error" in {
    addContextC().getError match {
      case InterceptError(err) => assert(err.getFullContext ==
        "Failure when calling addContextA\n" +
        "Failure when calling addContextB\n" +
        "My test error"
      )
      case _ => fail("Wrong error type returned")
    }
  }
}
