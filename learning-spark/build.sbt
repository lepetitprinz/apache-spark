ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.13.0"

lazy val root = (project in file("."))
  .settings(
    name := "learning-spark"
  )

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "3.2.3",
  "org.apache.spark" %% "spark-sql"  % "3.2.3"
)
