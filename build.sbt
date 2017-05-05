organization := "com.spotify"
name := "big-data-rosetta-code"
version := "0.1.0-SNAPSHOT"

scalaVersion := "2.11.11"
scalacOptions ++= Seq("-target:jvm-1.8", "-deprecation", "-feature", "-unchecked")
javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

resolvers ++= Seq(
  "conjars.org" at "http://conjars.org/repo"
)

libraryDependencies ++= Seq(
  "com.spotify" %% "scio-core" % "0.3.1",
  "com.spotify" %% "scio-extra" % "0.3.1",
  "com.spotify" %% "scio-test" % "0.3.1" % "test",
  "com.twitter" %% "scalding-core" % "0.16.0",
  "com.twitter" %% "algebird-spark" % "0.13.0",
  "org.apache.spark" %% "spark-core" % "1.6.3",
  "org.apache.spark" %% "spark-mllib" % "1.6.3",
  "org.scalacheck" %% "scalacheck" % "1.13.5" % "test",
  "com.storm-enroute" %% "scalameter" % "0.8.2" % "test"
)

val scalaMeterFramework = new TestFramework("org.scalameter.ScalaMeterFramework")
testFrameworks += scalaMeterFramework
testOptions += Tests.Argument(scalaMeterFramework, "-silent")
parallelExecution in Test := false
logBuffered := false
