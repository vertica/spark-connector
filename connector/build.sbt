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

scalaVersion := "2.12.0"
name := "spark-vertica-connector"
organization := "com.vertica"
version := "1.0"

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.vertica.jdbc" % "vertica-jdbc" % "10.0.1-0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.0.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.0.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "2.4.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "ch.qos.logback" % "logback-classic" % "1.2.3"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
libraryDependencies += "org.typelevel" %% "cats-core" % "2.3.0"

parallelExecution in Test := false

assemblyMergeStrategy in assembly := {
  case n if n.contains("services") => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

import sbtsonar.SonarPlugin.autoImport.sonarProperties

sonarProperties ++= Map(
  "sonar.host.url" -> "http://localhost:80",
  "sonar.coverage.exclusions" -> "**/*FileStoreLayerInterface.scala,**/*VerticaJdbcLayer.scala"
)

scapegoatVersion in ThisBuild := "1.3.3"
scapegoatReports := Seq("xml")
scalacOptions in Scapegoat += "-P:scapegoat:overrideLevels:all=Warning"
scalacOptions += "-Ypartial-unification"





