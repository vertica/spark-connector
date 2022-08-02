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

// Retrieving the common property config containing the connector version number.
val props = settingKey[Properties]("Connector version properties")
props := {
  val prop = new Properties()
  IO.load(prop, new File("../../version.properties"))
  prop
}

scalaVersion := "2.12.12"
name := "spark-vertica-connector-scala-example"
organization := "com.vertica"
version := props.value.getProperty("connector-version")

resolvers += "Artima Maven Repository" at "https://repo.artima.com/releases"
resolvers += "jitpack" at "https://jitpack.io"

libraryDependencies ++= Seq(
  "com.typesafe" % "config" % "1.4.1",
  "com.vertica.spark" % "vertica-spark" % s"${version.value}-slim",
  "org.apache.spark" %% "spark-core" % "3.3.0",
  "org.apache.spark" %% "spark-sql" % "3.3.0",

  // This version needs to match the Hadoop version used by Spark
  "org.apache.hadoop" % "hadoop-aws" % "3.3.2"
)
