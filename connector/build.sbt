scalaVersion := "2.12.0"
name := "spark-vertica-connector"
organization := "ch.epfl.scala"
version := "1.0"

resolvers += "Artima Maven Repository" at "http://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.vertica.jdbc" % "vertica-jdbc" % "10.0.1-0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
libraryDependencies += "com.github.brianfrankcooper.YCSB" % "core" % "0.17.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}


