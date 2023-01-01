package ingestion

import org.apache.spark.sql.SparkSession

object TextToDataframeApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Text to Dataframe")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("text")
      .load("data/ch07/romeo-juliet-pg1777.txt")

    df.show(10)
    df.printSchema

    spark.stop
  }
}
