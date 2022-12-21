package dataframe

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{concat, lit}

object SchemaIntrospectionScalaApp {
  def main(args: Array[String]): Unit = {
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Schema introspection")
      .master("local")
      .getOrCreate

    // Set log level
    spark.sparkContext.setLogLevel("ERROR")

    // Reads a csv file with header, stores it in a dataframe
    var df = spark.read
      .format("csv")
      .option("header", "true")
      .load("/Users/yjkim-studio/data/spark/Restaurants_in_Wake_County.csv")

    df = df.withColumn("county", lit("Wake"))
      .withColumnRenamed("HSISID", "datasetId")
      .withColumnRenamed("NAME", "name")
      .withColumnRenamed("ADDRESS1", "address1")
      .withColumnRenamed("ADDRESS2", "address2")
      .withColumnRenamed("CITY", "city")
      .withColumnRenamed("STATE", "state")
      .withColumnRenamed("POSTALCODE", "zip")
      .withColumnRenamed("PHONENUMBER", "tel")
      .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
      .withColumnRenamed("FACILITYTYPE", "type")
      .withColumnRenamed("X", "geoX")
      .withColumnRenamed("Y", "geoY")

    df = df.withColumn("id",
      concat(df.col("state"), lit("_"),
        df.col("county"), lit("_"), df.col("datasetId")))

    val schema = df.schema

    println("*** Schema as a tree: ")

    schema.printTreeString()
    val schemaAsString = schema.mkString
    println("*** Schema as string:" + schemaAsString)

    val schemaAsJson = schema.prettyJson
    println("*** Schema as JSON: " + schemaAsJson)

  }
}
