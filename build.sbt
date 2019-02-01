/*
 * Copyright 2016 Spotify AB.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import com.typesafe.sbt.SbtGit.GitKeys.gitRemoteRepo

organization := "com.spotify"
name := "big-data-rosetta-code"
version := "0.1.0-SNAPSHOT"

val scioVersion = "0.7.0"
val scaldingVersion = "0.17.4"
val sparkVersion = "2.4.0"
val algebirdVersion = "0.13.5"
val scalacheckVersion = "1.14.0"
val scalameterVersion = "0.10"

scalaVersion := "2.12.8"
scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-extra" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "com.twitter" %% "scalding-core" % scaldingVersion,
  "com.twitter" %% "algebird-spark" % algebirdVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
  "com.storm-enroute" %% "scalameter" % scalameterVersion % "test"
)

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")
testFrameworks += scalaMeterFramework
testOptions += Tests.Argument(scalaMeterFramework, "-silent")
parallelExecution in Test := false
logBuffered := false

scalacOptions ++= Seq(
  "-P:socco:out:target/socco",
  "-P:socco:package_com.spotify.scio:http://spotify.github.io/scio/api",
  "-P:socco:package_com.twitter.algebird:http://twitter.github.io/algebird/api",
  "-P:socco:package_com.twitter.scalding:http://twitter.github.io/scalding/api",
  "-P:socco:package_org.apache.spark:http://spark.apache.org/docs/latest/api/scala"
)
autoCompilerPlugins := true
addCompilerPlugin("com.criteo.socco" %% "socco-plugin" % "0.1.9")

lazy val soccoIndex = taskKey[File]("Generates examples/index.html")
soccoIndex := SoccoIndex.generate(target.value / "socco" / "index.html")
compile in Compile := {
  soccoIndex.value
  (compile in Compile).value
}
mappings in makeSite ++= SoccoIndex.mappings
gitRemoteRepo := "git@github.com:spotify/big-data-rosetta-code.git"

enablePlugins(GhpagesPlugin)
