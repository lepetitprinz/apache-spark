package ingestion.file.csv

import ingestion.utils.SchemaInspector
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DataTypes, StructField}

object ComplexCsvToDataframeWithSchemaApp {

  def main(args: Array[String]): Unit = {
    // creates a session on a local master
    val spark = SparkSession.builder
      .appName("Complex csv with schema to dataframe")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    // creates the schema
    val schema = DataTypes.createStructType(Array[StructField](
      DataTypes.createStructField("id", DataTypes.IntegerType, false),
      DataTypes.createStructField("authorId", DataTypes.IntegerType, true),
      DataTypes.createStructField("bookTitle", DataTypes.StringType, false),
      DataTypes.createStructField("releaseDate", DataTypes.StringType, true),
      DataTypes.createStructField("url", DataTypes.StringType, false)))

    SchemaInspector.print(schema)

    // reads a csv file with header
    val df = spark.read.format("csv")
      .option("header", "true")
      .option("multiline", true)
      .option("sep", ";")
      .option("quote", "*")
      .schema(schema)
      .load("data/ch07/books.csv")

    SchemaInspector.print(Some("Schema ....."), schema)
    SchemaInspector.print("Dataframe ...", df)

    df.show(30, 25, false)
    df.printSchema()
  }
}
