import org.apache.spark.sql.SparkSession

object CsvToDataframeApp {
  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("CSV to Dataset")
      .master("local[*]")
      .getOrCreate

    // Reads a CSV file with header, called books.csv, stores it in a dataframe
    val df = spark.read.format("csv")
      .option("header", "true")
      .load("data/books.csv")

    // Shows at most 5 rows from the dataframe
    df.show(5)

    // Good to stop SparkSession at the end of the application
    spark.stop
  }

}