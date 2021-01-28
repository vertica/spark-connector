package com.vertica.spark.datasource.core

/**
 * Interface for getting a unique session ID
 */
trait SessionIdInterface {
  def getId : String
}

object SessionId extends SessionIdInterface {
  def getId : String = java.util.UUID.randomUUID.toString
}
