scalaVersion := "2.12.12"
name := "spark-vertica-connector-functional-tests"
organization := "ch.epfl.scala"
version := "1.0"

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
