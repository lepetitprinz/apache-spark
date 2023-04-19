import org.apache.spark.sql.SparkSession

object FlightsSQLApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder
      .appName("FlightsSQLApp")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val csvFile = "data/flights/departuredelays.csv"
    val schema = "date STRING, delay INT, distance INT, " +
      "origin STRING, destination STRING"

    val df = spark.read.format("csv")
      .option("header", "true")
      .option("schema", schema)
      .load(csvFile)

    // create a temporary view
    df.createOrReplaceTempView("us_delay_flights_tbl")

    spark.sql(
      """SELECT distance, origin, destination
    FROM us_delay_flights_tbl WHERE distance > 1000
    ORDER BY distance DESC""").show(10)

    spark.sql(
      """SELECT date, delay, origin, destination
    FROM us_delay_flights_tbl
    WHERE delay > 120 AND ORIGIN = 'SFO' AND DESTINATION = 'ORD'
    ORDER by delay DESC""").show(10)

    spark.sql(
      """SELECT delay, origin, destination,
    CASE
    WHEN delay > 360 THEN 'Very Long Delays'
    WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
    WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
    WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
    WHEN delay = 0 THEN 'No Delays'
    ELSE 'Early'
    END AS Flight_Delays
    FROM us_delay_flights_tbl
    ORDER BY origin, delay DESC""").show(10)
  }

}
