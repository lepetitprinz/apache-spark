package ingestion.database.dialect

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc.JdbcDialects

object InformixToDatasetApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Informix to Dataframe using a JDBC Connection")
      .master("local[*]")
      .getOrCreate

    val dialect = new InformixJdbcDialect
    JdbcDialects.registerDialect(dialect)

    val informixURL = "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y"

    // Using properties
    val df = spark.read
      .format("jdbc")
      .option("url", informixURL)
      .option("dbtable", "customer")
      .option("user", "informix")
      .option("password", "informix")
      .load

    df.show(5)
    df.printSchema
    
    spark.stop
  }

}
