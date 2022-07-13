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
import java.util.Properties

// Retrieving the connector version number from a common file.
val versionProps = settingKey[Properties]("Connector version properties")
versionProps := {
  val prop = new Properties()
  IO.load(prop, new File("../version.properties"))
  prop
}

scalaVersion := "2.12.12"
name := "spark-vertica-connector-functional-tests"
organization := "com.vertica"
version := versionProps.value.getProperty("connector-version")

val sparkVersion = Option(System.getProperty("sparkVersion")) match {
  case Some(sparkVersion) => sparkVersion
  case None => sys.env.getOrElse("SPARK_VERSION", "3.3.0")
}

val hadoopAwsVersion = Option(System.getProperty("HADOOP_AWS_VERSION")) match {
  case Some(hadoopAws) => hadoopAws
  case None => sys.env.getOrElse("HADOOP_VERSION", "3.3.2")
}

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2"
libraryDependencies += "com.typesafe" % "config" % "1.4.1"

libraryDependencies += "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"
libraryDependencies += "com.vertica.jdbc" % "vertica-jdbc" % "11.0.2-0"
libraryDependencies += "org.apache.spark" %% "spark-core" % sparkVersion
libraryDependencies += "org.apache.spark" %% "spark-sql" % sparkVersion
libraryDependencies += "org.scalactic" %% "scalactic" % "3.2.2"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.2.2" % "test"
libraryDependencies += "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
libraryDependencies += "org.scalamock" %% "scalamock" % "4.4.0" % Test
libraryDependencies += "org.typelevel" %% "cats-core" % "2.3.0"
libraryDependencies += "org.apache.hadoop" % "hadoop-aws" % hadoopAwsVersion
libraryDependencies += "com.github.scopt" %% "scopt" % "4.0.1"
libraryDependencies += "com.google.cloud.bigdataoss" % "gcs-connector" % "hadoop3-2.2.6"

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}

assembly / assemblyShadeRules := Seq(
  ShadeRule.rename("cats.**" -> "shadeCats.@1").inAll
)

//unmanagedClasspath in Runtime += new File("/etc/hadoop/conf/")

