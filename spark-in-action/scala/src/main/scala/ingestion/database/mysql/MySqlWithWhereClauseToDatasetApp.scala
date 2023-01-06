package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object MySqlWithWhereClauseToDatasetApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL with where clause to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "mysql")
    props.put("useSSL", "false")
    props.put("allowPublicKeyRetrieval", "true")

    val sqlQuery = "select * from film where " +
    "(title like \"%ALIEN%\" or title like \"%victory%\") " +
    "and rental_rate>1 "

    val mySqlUrl = "jdbc:mysql://localhost:3306/sakila"

    val df = spark.read
      .jdbc(mySqlUrl, "(" + sqlQuery + ") film_alias", props)

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} records")

    spark.stop
  }
}
