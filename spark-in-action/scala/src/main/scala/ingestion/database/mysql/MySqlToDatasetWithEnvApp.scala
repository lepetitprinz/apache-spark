package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object MySqlToDatasetWithEnvApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("useSSL", "false")

    var password = System.getenv("DB_PASSWORD").getBytes
    props.put("password", new String(password))
    password = null

    val mySqlUrl = "jdbc:mysql://localhost:3306/sakila"
    var df = spark.read.jdbc(mySqlUrl, "actor", props)

    df = df.orderBy(df.col("last_name"))

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} record(s)")

    spark.stop
  }

}
