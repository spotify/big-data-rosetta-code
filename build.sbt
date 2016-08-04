organization := "com.spotify"
name := "big-data-rosetta-code"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.8"
scalacOptions ++= Seq("-target:jvm-1.7", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.7", "-target", "1.7")

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.1.10",
  "com.spotify" %% "scio-extra" % "0.1.10",
  "com.spotify" %% "scio-test" % "0.1.10" % "test",
  "com.twitter" %% "scalding-core" % "0.16.0",
  "com.twitter" %% "algebird-spark" % "0.12.1",
  "org.apache.spark" %% "spark-core" % "1.6.2",
  "org.apache.spark" %% "spark-mllib" % "1.6.2",
  "org.scalacheck" %% "scalacheck" % "1.13.2" % "test"
)
