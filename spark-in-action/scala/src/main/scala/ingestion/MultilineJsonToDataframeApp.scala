package ingestion

import org.apache.spark.sql.SparkSession

object MultilineJsonToDataframeApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Multiline Json to Dataframe")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("json")
      .option("multiline", true)
      .load("data/ch07/countrytravelinfo.json")

    df.show(3)
    df.printSchema

    spark.stop
  }

}
