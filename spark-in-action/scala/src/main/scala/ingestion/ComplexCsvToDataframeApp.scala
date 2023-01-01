package ingestion

import org.apache.spark.sql.SparkSession

object ComplexCsvToDataframeApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Complex csv to Dataframe")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    println("Using Apache Spark v" + spark.version)

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ';')
      .option("quote", "*")
      .option("dateFormat", "MM/dd/yyyy")
      .option("inferSchema", true)
      .load("data/ch07/books.csv")

    println("Excerpt of the dataframe content:")

    df.show(7, 70)
    println("Dataframe's schema:")
    df.printSchema()
  }

}
