name := "akka-to-kafka"

version := "1.0"

scalaVersion := "2.12.6"

lazy val akkaVersion = "2.5.19"
lazy val betterFilesVersion = "3.7.0"
lazy val sprayVersion = "1.3.5"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % akkaVersion,
  "com.typesafe.akka" %% "akka-testkit" % akkaVersion,
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  "com.github.pathikrit" %% "better-files" % betterFilesVersion,
  "io.spray" %%  "spray-json" % sprayVersion
)
