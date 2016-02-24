organization := "com.spotify"

name := "big-data-rosetta-code"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.7"

scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked")

javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq(
  "com.twitter" %% "scalding-core" % "0.15.0",
  "com.twitter" %% "algebird-spark" % "0.12.0",
  "org.apache.spark" %% "spark-core" % "1.6.0",
  "org.apache.spark" %% "spark-mllib" % "1.6.0"
)
