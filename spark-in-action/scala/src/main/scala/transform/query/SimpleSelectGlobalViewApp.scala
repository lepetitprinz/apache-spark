package transform.query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object SimpleSelectGlobalViewApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Simple SELECT using SQL")
      .master("local")
      .getOrCreate

    val schema = DataTypes.createStructType(Array[StructField]
      (DataTypes.createStructField("geo", DataTypes.StringType, true),
       DataTypes.createStructField("yr1980", DataTypes.DoubleType, false))
    )

    val df = spark.read
      .format("csv")
      .option("header", true)
      .schema(schema)
      .load("data/ch11/populationbycountry19802010millions.csv")

    df.createOrReplaceGlobalTempView("geodata")
    df.printSchema()

    val query1 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
        |""".stripMargin

    val smallCOuntriesDf = spark.sql(query1)

    smallCOuntriesDf.show(10, false)

    val query2 =
      """
        |SELECT * FROM global_temp.geodata
        |WHERE yr1980 >= 1
        |ORDER BY 2
        |LIMIT 5
        |""".stripMargin

    val spark2 = spark.newSession
    val slightlyBiggerCountriesDf = spark.sql(query2)

    slightlyBiggerCountriesDf.show(10, false)

    spark.stop
  }

}
