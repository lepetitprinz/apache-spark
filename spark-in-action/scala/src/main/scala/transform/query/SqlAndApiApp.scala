package transform.query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions
import org.apache.spark.sql.types.{DataTypes, StructField}

object SqlAndApiApp {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple SQL")
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

    // Creates a new column with the evolution of the population between 1980 and 2010
    df = df.withColumn("evolution", functions.expr("round((yr2010 - yr1980) * 1000000)"))
    df.createOrReplaceTempView("geodata")

    val query1 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution <= 0
        |ORDER BY evolution
        |LIMIT 25
        |""".stripMargin

    val negativeEvolutionDf = spark.sql(query1)
    negativeEvolutionDf.show(15, truncate = false)

    val query2 =
      """
        |SELECT * FROM geodata
        |WHERE geo IS NOT NULL AND evolution > 999999
        |ORDER BY evolution DESC
        |LIMIT 25
        |""".stripMargin

    val moreThanAMillionDf = spark.sql(query2)
    moreThanAMillionDf.show(15, truncate = false)

    spark.stop
  }

}
