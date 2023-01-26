package transform.query

import org.apache.spark.sql.{SparkSession}

object QueryOnJsonApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Query on JSON")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("json")
      .option("multiline", "true")
      .load("data/ch12/json/store.json")
  }

}
