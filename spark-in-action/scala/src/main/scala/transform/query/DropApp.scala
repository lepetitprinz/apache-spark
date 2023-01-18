package transform.query

import org.apache.spark.sql.{SparkSession, functions => F}
import org.apache.spark.sql.types.{DataTypes, StructField}
import org.slf4j.LoggerFactory

object DropApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple SQL using 'not' operator")
      .master("local")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("geo", DataTypes.StringType, true),
      DataTypes.createStructField("yr1980", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1981", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1982", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1983", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1984", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1985", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1986", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1987", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1988", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1989", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1990", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1991", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1992", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1993", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1994", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1995", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1996", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1997", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1998", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr1999", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2000", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2001", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2002", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2003", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2004", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2005", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2006", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2007", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2008", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2009", DataTypes.DoubleType, false),
      DataTypes.createStructField("yr2010", DataTypes.DoubleType, false)
    ))

    var df = spark.read
      .format("csv")
      .option("header", value = true)
      .schema(schema)
      .load("data/ch11/populationbycountry19802010millions.csv")

    for (i <- Range(1981, 2010))
      df = df.drop(df.col("yr" + i))

    df = df.withColumn("evolution", F.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")

    val query =
      """
        |SELECT *
        |FROM geodata
        |WHERE geo is not null
        |AND geo NOT IN ('Africa', 'North America', 'World', 'Asia & Oceania',
        |'Central & South America', 'Europe', 'Eurasia', 'Middle East')
        |ORDER BY yr2010 DESC
        |""".stripMargin

    val cleanedDf = spark.sql(query)
    cleanedDf.show(20, truncate = false)

    spark.stop
  }
}
