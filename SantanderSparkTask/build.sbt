ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.16"

val sparkVersion = "3.5.4"

lazy val root = (project in file("."))
  .settings(
    name := "SantanderSparkTask"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "com.lihaoyi" %% "os-lib" % "0.11.4",
  "org.scalatest" %% "scalatest" % "3.2.19",
  "com.github.mrpowers" %% "spark-fast-tests" % "1.3.0"
)