name := "spark-in-action"

version := "1.0"

scalaVersion := "2.13.10"

val sparkVersion = "3.2.3"
val sparkXmlVersion = "0.15.0"
val sparkAvroVersion = "3.2.3"
val mysqlVersion = "8.0.30"
val informixJdbcVersion = "4.50.8"
val elasticSearchHadoopVersion = "8.5.3"

libraryDependencies ++= Seq(
  "org.apache.spark"  %% "spark-core"           % sparkVersion,
  "org.apache.spark"  %% "spark-sql"            % sparkVersion,
  "org.apache.spark"  %% "spark-avro"           % sparkAvroVersion,
  "com.databricks"    %% "spark-xml"            % sparkXmlVersion,
  "mysql"             % "mysql-connector-java"  % mysqlVersion,
 // "com.ibm.informix"  % "jdbc"                  % informixJdbcVersion,
 // "org.elasticsearch" % "elasticsearch-hadoop"  % elasticSearchHadoopVersion
)
