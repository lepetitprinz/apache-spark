package ingestion.file.json

import org.apache.spark.sql.SparkSession

object JsonLinesToDataframeApp {

  def main(args: Array[String]): Unit = {
    // create a session on a local master
    val spark = SparkSession.builder
      .appName("Json Lines to Dataframe")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read.format("json")
      .load("data/ch07/durham-nc-foreclosure-2006-2016.json")
    
    df.show(5)
    df.printSchema

    spark.stop
  }

}
