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

scalaVersion := "2.11.12"
scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers ++= Seq(
  "conjars.org" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.5.1",
  "com.spotify" %% "scio-extra" % "0.5.1",
  "com.spotify" %% "scio-test" % "0.5.1" % "test",
  "com.twitter" %% "scalding-core" % "0.17.3",
  "com.twitter" %% "algebird-spark" % "0.13.0",
  "org.apache.spark" %% "spark-core" % "2.1.2",
  "org.apache.spark" %% "spark-mllib" % "2.1.2",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "com.storm-enroute" %% "scalameter" % "0.8.2" % "test"
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
