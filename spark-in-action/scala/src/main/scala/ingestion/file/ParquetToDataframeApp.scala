package ingestion.file

import org.apache.spark.sql.SparkSession

object ParquetToDataframeApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Parquet to Dataframe")
      .master("local[*]")
      .getOrCreate

    val df = spark.read
      .format("parquet")
      .load("data/ch07/alltypes_plain.parquet")

    df.show(10)
    df.printSchema()
    println(s"The dataframe has ${df.count} rows")

    spark.stop
  }

}
