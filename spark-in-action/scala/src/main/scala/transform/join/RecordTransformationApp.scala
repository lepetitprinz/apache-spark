package transform.join

import org.apache.spark.sql.{SparkSession, functions => F}

object RecordTransformationApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Record Transformation")
      .master("local[*]")
      .getOrCreate

    var intermediateDf = spark.read
      .format("csv")
      .option("header", value = true)
      .option("inferSchema", value = true)
      .load("data/ch12/census/PEP_2017_PEPANNRES.csv")

    intermediateDf = intermediateDf
      .drop("GEO.id")
      .withColumnRenamed("GEO.id2", "id")
      .withColumnRenamed("GEO.display-label", "label")
      .withColumnRenamed("rescen42010", "real2010")
      .drop("resbase42010")
      .withColumnRenamed("respop72010", "est2010")
      .withColumnRenamed("respop72011", "est2011")
      .withColumnRenamed("respop72012", "est2012")
      .withColumnRenamed("respop72013", "est2013")
      .withColumnRenamed("respop72014", "est2014")
      .withColumnRenamed("respop72015", "est2015")
      .withColumnRenamed("respop72016", "est2016")
      .withColumnRenamed("respop72017", "est2017")

    intermediateDf.printSchema()
    intermediateDf.show(5)

    // Creates the additional columns
    intermediateDf = intermediateDf
      .withColumn("countyState", F.split(F.col("label"), ", "))
      .withColumn("stateId", F.expr("int(id/1000)"))
      .withColumn("countyId", F.expr("id%1000"))

    intermediateDf.printSchema()
    intermediateDf.sample(.01).show(5, truncate = false)

    intermediateDf = intermediateDf
      .withColumn("state", F.col("countyState").getItem(1))
      .withColumn("county", F.col("countyState").getItem(0))
      .drop("countyState")

    intermediateDf.printSchema()
    intermediateDf.sample(.01).show(5, truncate = false)

    // Performs some statistics on the intermediate dataframe
    var statDf = intermediateDf
      .withColumn("diff", F.expr("est2010-real2010"))
      .withColumn("growth", F.expr("est2017-est2010"))
      .drop("id", "label", "real2010", "est2010", "est2011", "est2012",
      "est2013", "est2014", "est2015", "est2016", "est2017")

    statDf.printSchema()
    statDf.sample(.01).show(5, false)

    spark.stop
  }
}
