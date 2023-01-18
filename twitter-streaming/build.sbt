name := "twitter-streaming"

version := "0.1.0"

scalaVersion := "2.11.12"

val sparkVersion = "2.4.8"
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.apache.spark" %% "spark-streaming-twitter" % "1.6.3",
  "org.apache.commons" % "commons-lang3" % "3.12.0",
 // "org.slf4j" % "slf4j-api" % "2.0.6",
  "com.typesafe" % "config" % "1.3.4",
  "com.github.scopt" %% "scopt" % "4.1.0"
)