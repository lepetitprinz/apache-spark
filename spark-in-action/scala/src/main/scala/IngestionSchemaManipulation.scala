import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.functions.concat

object IngestionSchemaManipulation {
  def main(args: Array[String]): Unit = {
    // Create a spark session
    val spark = SparkSession.builder
      .appName("Restaurants in Wake Country, NC")
      .master("local[*]")
      .getOrCreate()

    // Read a csv file
    var df = spark.read.format("csv")
      .option("header", "true")
      .load("/Users/yjkim-studio/data/spark/Restaurants_in_Wake_County.csv")

    println("*** Right after Ingestion")

    df.show(5)
    df.printSchema()
    println("records " + df.count)

    // Transform dataframe
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
      .drop("OBJECTID", "PERMITID", "GEOCODESTATUS")

      df = df.withColumn("id",
        concat(df.col("state"), lit("_"), df.col("county"), lit("_"), df.col("datasetId")))

      println("*** dataframe transformed")
    df.show(5)

    // for book only
    val dropCols = List("address2", "zip", "tel", "dataStart", "geoX", "geoY", "address1", "datasetId")
    val dfUsedForBook = df.drop(dropCols:_*)
    dfUsedForBook.show(5, 15)

    println("*** Looking at partitions")
    val partitions = df.rdd.partitions
    val partitionCount = partitions.length
    println("Partition count before repartition: " + partitionCount)

    df = df.repartition(4)
    println("Partition count after repartition: " + df.rdd.partitions.length)
  }
}
