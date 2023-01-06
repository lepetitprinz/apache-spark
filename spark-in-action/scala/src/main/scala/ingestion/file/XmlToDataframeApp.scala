package ingestion.file

import org.apache.spark.sql.SparkSession

object XmlToDataframeApp {

  def main(args: Array[String]): Unit = {
    // create a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("XML to Dataframe")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("xml")
      .option("rowTag", "row")
      .load("data/ch07/nasa-patents.xml")

    df.show(5)
    df.printSchema

    spark.stop
  }
}
