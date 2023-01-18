package transform.query

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object SimpleSelectApp {

  def main(args: Array[String]): Unit = {

    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Simple SELECT using SQL")
      .master("local")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    val schema = DataTypes.createStructType(Array[StructField]
      (DataTypes.createStructField("geo", DataTypes.StringType, true),
       DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)))

    val df = spark.read
      .format("csv")
      .schema(schema)
      .load("data/ch11/populationbycountry19802010millions.csv")

    df.createOrReplaceTempView("geodata")
    df.printSchema()

    val query =
      """
        |SELECT * FROM geodata
        |WHERE yr1980 < 1
        |ORDER BY 2
        |LIMIT 5
        |""".stripMargin

    val smallCountries = spark.sql(query)

    // Shows at most 10 rows from the dataframe
    smallCountries.show(10, truncate = false)

    spark.stop
  }
}
