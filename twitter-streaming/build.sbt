name := "twitter-streaming"

version := "0.1.0"

scalaVersion := "2.13.10"

val sparkVersion = "3.2.3"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion,
  "org.apache.spark" %% "spark-sql" % sparkVersion,
  "org.apache.spark" %% "spark-streaming" % sparkVersion,
  "org.mongodb.spark" %% "mongo-spark-connector" % "10.1.0",
)