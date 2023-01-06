package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

object MySqlToDatasetWithOptionsApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using a JDBC Connection with options")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    var df = spark.read
      .option("url", "jdbc:mysql://localhost:3306/sakila")
      .option("dbtable", "actor")
      .option("user", "root")
      .option("password", "mysql")
      .option("useSSL", "false")
      .option("allowPublicKeyRetrieval", "true")
      .format("jdbc")
      .load

    df = df.orderBy(df.col("last_name"))

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} record(s)")

    spark.stop
  }

}
