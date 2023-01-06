package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object MySqlToDatasetApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val props = new Properties
    props.put("user", "root")
    props.put("password", "mysql")
    props.put("useSSL", "false")
    props.put("allowPublicKeyRetrieval", "true")

    val mysqlUrl = "jdbc:mysql://localhost:3306/sakila"

    var df = spark.read.jdbc(mysqlUrl, "actor", props)

    df = df.orderBy(df.col("last_name"))

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} records(s)")

    spark.stop
  }
}
