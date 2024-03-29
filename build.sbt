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

import com.github.sbt.git.SbtGit.GitKeys.gitRemoteRepo
import _root_.io.regadas.sbt.SbtSoccoKeys._

organization := "com.spotify"
name := "big-data-rosetta-code"
version := "0.1.0-SNAPSHOT"

val scioVersion = "0.13.5"
val scaldingVersion = "0.17.4"
val sparkVersion = "3.5.0"
val algebirdVersion = "0.13.10"
val scalacheckVersion = "1.17.0"
val scalameterVersion = "0.19"
val scalatestVersion = "3.2.17"
val scalatestPlusVersion = "3.2.17.0"

scalaVersion := "2.12.18"
scalacOptions ++= Seq(
  "-target:jvm-1.8",
  "-deprecation",
  "-feature",
  "-unchecked",
  "-language:higherKinds"
)
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers += "Cascading libraries" at "https://conjars.wensel.net/repo/"
libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % scioVersion,
  "com.spotify" %% "scio-extra" % scioVersion,
  "com.spotify" %% "scio-test" % scioVersion % "test",
  "com.twitter" %% "scalding-core" % scaldingVersion,
  "com.twitter" %% "algebird-spark" % algebirdVersion,
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-mllib" % sparkVersion,
  "org.scalatest" %% "scalatest" % scalatestVersion % "test",
  "org.scalatestplus" %% "scalacheck-1-17" % scalatestPlusVersion % "test",
  "org.scalacheck" %% "scalacheck" % scalacheckVersion % "test",
  "com.storm-enroute" %% "scalameter" % scalameterVersion % "test"
)

val scalaMeterFramework = new TestFramework(
  "org.scalameter.ScalaMeterFramework"
)
testFrameworks += scalaMeterFramework
testOptions += Tests.Argument(scalaMeterFramework, "-silent")
Test / parallelExecution := false
logBuffered := false

soccoOnCompile := true
soccoPackage := List(
  "com.spotify.scio:http://spotify.github.io/scio/api",
  "com.twitter.algebird:http://twitter.github.io/algebird/api",
  "com.twitter.scalding:http://twitter.github.io/scalding/api",
  "org.apache.spark:http://spark.apache.org/docs/latest/api/scala"
)
addCompilerPlugin(("io.regadas" %% "socco-ng" % "0.1.4").cross(CrossVersion.full))
makeSite := makeSite.dependsOn(Compile / compile).value
gitRemoteRepo := "git@github.com:spotify/big-data-rosetta-code.git"

enablePlugins(SbtSoccoPlugin)
enablePlugins(GhpagesPlugin)
