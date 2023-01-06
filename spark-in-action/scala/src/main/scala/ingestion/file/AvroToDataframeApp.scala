package ingestion.file

import org.apache.spark.sql.SparkSession

object AvroToDataframeApp {

  def main(args: Array[String]): Unit = {

    // create a session on a local master
    val spark: SparkSession = SparkSession.builder
      .appName("Avro to Dataframe")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext

  }
}
