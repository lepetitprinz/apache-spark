import org.apache.spark.sql.SparkSession

object HigherOrderFunctions {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder
      .appName("Higher-Order Functions")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.implicits._
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")
    tC.createOrReplaceTempView("tC")
    tC.show()

    // transform
    val transformDF = spark.sql(
      """
        |SELECT celsius
        |     , transform(celsius, t -> (((t * 9) div 5) + 32)) AS fahrenheit
        |  FROM tC
        |""".stripMargin)
    transformDF.show()

    // filter
    val filterDF = spark.sql(
      """
        |SELECT celsius
        |     , filter(celsius, t -> t > 38) AS high
        |  FROM tC
        |""".stripMargin)
    filterDF.show()

    // exists
    val existsDF = spark.sql(
      """
        |SELECT celsius
        |     , exists(celsius, t -> t = 38) AS threshold
        |  FROM tC
        |""".stripMargin)
    existsDF.show()

    // reduce
    val reduceDF = spark.sql("SELECT aggregate(array(1, 2, 3), 0, (acc, x) -> acc + x) AS rd")
    reduceDF.show()
  }
}
