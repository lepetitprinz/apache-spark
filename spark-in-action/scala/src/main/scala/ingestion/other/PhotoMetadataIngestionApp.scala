package ingestion.other

import org.apache.spark.sql.SparkSession


/**
 * Ingest metadata from a directory containing photos, make them available as EXIF.
 *
 */
object PhotoMetadataIngestionApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("EXIF to Dataset")
      .master("local")
      .getOrCreate

    val importDirectory = "data/ch09"

    val df = spark.read
      .format("exif")
      .option("recursive", "true")
      .option("limit", "100000")
      .option("extensions", "jpg,jpeg")
      .load(importDirectory)

    println("imported " + df.count + " photos.")
    df.printSchema()
    df.show(5)
  }
}
