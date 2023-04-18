import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
object MnMCount {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("MnMCount")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")
//    if (args.length < 1) {
//      print("Usage : MnMCount <mnm_file_dataset>")
//      sys.exit(1)
//    }

    // get the M&M data set file name
    val mnmfile = "data/mnm_dataset.csv"

    // read the file into a Spark DataFrame
    val mnmDF = spark.read.format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load(mnmfile)

    // display the DataFrame
    mnmDF.show(5, false)

    // aggregate count of all colors and groupBy state and color
    // orderBy descending order
    val countMnMDF = mnmDF.select("State", "Color", "Count")
      .groupBy("State", "Color")
      .sum("Count")
      .orderBy(desc("sum(Count)"))

    // show all the resulting aggregation for all the dates and colors
    countMnMDF.show(10)
    println(s"Total Rows = ${countMnMDF.count()}")
    println()

    // find the aggregate count for California by filtering
    val caCountMnMDF = mnmDF.select("*")
      .where(col("State") === "CA")
      .groupBy("State", "Color")
      .sum("count")
      .orderBy(desc("sum(Count)"))

    caCountMnMDF.explain(true)

    // show the resulting aggregation for California
    caCountMnMDF.show(10)

    // stop the SparkSession
    spark.stop()
  }
}
