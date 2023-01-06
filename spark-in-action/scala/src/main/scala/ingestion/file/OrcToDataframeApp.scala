package ingestion.file

import org.apache.spark.sql.SparkSession

object OrcToDataframeApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("ORC to Dataframe")
      .config("spark.sql.orf.imlp", "native")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("orc")
      .load("data/ch07/demo-l1-zlib.orc")

    df.show(10)
    df.printSchema()

    println(s"The dataframe has ${df.count} rows")

    spark.stop
  }

}
