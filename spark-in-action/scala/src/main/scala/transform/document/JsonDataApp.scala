package transform.document

import org.apache.spark.sql.{DataFrame, SparkSession}

class JsonDataApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Display JSON raw data")
      .master("local")
      .getOrCreate

    val df: DataFrame = spark.read
      .format("json")
      .option("multiline", "true")
      .load("data/ch13/json/shipment.json")

    df.show(5, 16)
    df.printSchema()

    spark.stop
  }
}
