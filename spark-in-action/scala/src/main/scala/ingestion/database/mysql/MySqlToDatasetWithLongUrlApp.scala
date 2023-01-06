package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object MySqlToDatasetWithLongUrlApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    // Using a JDBC URL
    val jdbcUrl = "jdbc:mysql://localhost:3306/sakila" +
      "?user=root" +
      "&password=mysql" +
      "&useSSL=false" +
      "&allowPublicKeyRetrieval=true"

    var df = spark.read.jdbc(jdbcUrl, "actor", new Properties)
    df = df.orderBy(df.col("last_name"))

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} record(s)")

    spark.stop
  }
}
