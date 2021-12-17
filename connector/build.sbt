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

scalaVersion := "2.12.12"
name := "spark-vertica-connector"
organization := "com.vertica"
version := "3.0.1"

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.vertica.jdbc" % "vertica-jdbc" % "10.0.1-0"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-hdfs" % "3.3.0"
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2" % Test
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
libraryDependencies += "org.typelevel" %% "cats-core" % "2.1.1"

Test / parallelExecution := false

assembly / assemblyMergeStrategy := {
  case n if n.contains("services") => MergeStrategy.concat
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

import sbtsonar.SonarPlugin.autoImport.sonarProperties

sonarProperties ++= Map(
  "sonar.host.url" -> "http://localhost:80",
)

ThisBuild / scapegoatVersion := "1.3.3"
scapegoatReports := Seq("xml")
Scapegoat / scalacOptions += "-P:scapegoat:overrideLevels:all=Warning"
scalacOptions += "-Ypartial-unification"
scalacOptions += "-Ywarn-value-discard"

scalastyleFailOnError := true
scalastyleFailOnWarning := true

// Explanation for exclusions from unit test coverage:
// - JDBC Layer: excluded as a bottom-layer component that does IO -- covered by integration tests.
// - File Store Layer: excluded as a bottom-layer component that does IO -- covered by integration tests.
// - Pipe Factory: as the rest of the components rely on abstract interfaces, this is the place
//   that creates the concrete implementations of those, such as the bottom-layer components mentioned above.
// - Parquet reader files taken from third part spark library
coverageExcludedPackages := "<empty>;.*jdbc.*;.*fs.*;.*core.factory.*;.*parquet.*"
coverageMinimum := 80
coverageFailOnMinimum := true

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("cats.**" -> "shadeCats.@1").inAll
)

assembly / assemblyExcludedJars := {
  val cp = (assembly / fullClasspath).value
  cp.filter{f => f.data.getName.contains("spark") ||
              f.data.getName.contains("hadoop")
  }
}

Test / fork := true

Test / envVars := Map(
  "AWS_ACCESS_KEY_ID" -> "test",
  "AWS_SECRET_ACCESS_KEY" -> "foo",
  "AWS_SESSION_TOKEN" -> "testsessiontoken",
  "AWS_DEFAULT_REGION" -> "us-west-1",
  "AWS_CREDENTIALS_PROVIDER" -> "test-provider"
)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  settings(
    buildInfoKeys := Seq[BuildInfoKey](name, version, scalaVersion, sbtVersion),
    buildInfoPackage := "buildinfo"
  )
