scalaVersion := "2.12.12"
name := "spark-vertica-connector-functional-tests"
organization := "com.vertica"
version := "1.0"

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "com.typesafe" % "config" % "1.4.1"
