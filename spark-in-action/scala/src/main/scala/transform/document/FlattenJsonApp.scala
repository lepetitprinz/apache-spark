package transform.document

import org.apache.spark.sql.{SparkSession, DataFrame, functions => F}

object FlattenJsonApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Display Flattened JSON data")
      .master("local")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .format("json")
      .option("multiline", "true")
      .load("data/ch13/json/shipment.json")

    val df2 = df
      .withColumn("supplier_name", F.col("supplier.name"))
      .withColumn("supplier_city", F.col("supplier.city"))
      .withColumn("supplier_state", F.col("supplier.state"))
      .withColumn("supplier_country", F.col("supplier.country"))
      .drop("supplier")
      .withColumn("customer_name", F.col("customer.name"))
      .withColumn("customer_city", F.col("customer.city"))
      .withColumn("customer_state", F.col("customer.state"))
      .withColumn("customer_country", F.col("customer.country"))
      .drop("customer")
      .withColumn("items", F.explode(F.col("books")))

    val df3 = df2
      .withColumn("qty", F.col("items.qty"))
      .withColumn("title", F.col("items.title"))
      .drop("items", "books")

    df3.show(5, false)
    df3.printSchema()

    df3.createOrReplaceTempView("shipment_detail")

    val sqlQuery =
      """
        |SELECT COUNT(*) AS bookCount
        |FROM shipment_detail
        |""".stripMargin
    val bookCountDf = spark.sql(sqlQuery)

    bookCountDf.show

    spark.stop
  }

}
