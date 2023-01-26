package transform.document

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession, functions => F}

object AutoFlattenJsonApp {
  val ARRAY_TYPE = "Array"
  val STRUCT_TYPE = "Struc"

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Automatic flattening of a JSON document")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val invoicesDf = spark.read
      .format("json")
      .option("multiline", "true")
      .load("data/ch13/json/nested_array.json")

    invoicesDf.show(3)
    invoicesDf.printSchema

    val flatInvoicesDf: Dataset[Row] = flattenNestedStructure(spark, invoicesDf)
    flatInvoicesDf.show(20, false)
    flatInvoicesDf.printSchema

    spark.stop
  }
  def flattenNestedStructure(spark: SparkSession, df: DataFrame): DataFrame = {
    var recursion = false
    var processedDf = df
    val schema = df.schema
    val fields = schema.fields

    for (field <- fields) {
      field.dataType.toString.substring(0, 5) match {
        case ARRAY_TYPE =>
          // Explodes array
          processedDf = processedDf.withColumnRenamed(field.name, field.name + "_tmp")
          processedDf = processedDf.withColumn(
            field.name,
            F.explode(F.col(field.name + "_tmp"))
          )
          processedDf = processedDf.drop(field.name + "_tmp")
          recursion = true
        case STRUCT_TYPE =>
          val ddl = field.toDDL.split("`")
          var i = 3
          while (i < ddl.length) {
            processedDf = processedDf.withColumn(
              field.name + "_" + ddl(i),
              F.col(field.name + "." + ddl(i)))
            i += 2
          }
          processedDf = processedDf.drop(field.name)
          recursion = true

        case _ =>
          processedDf = processedDf
      }
    }

    if (recursion)
      processedDf = flattenNestedStructure(spark, processedDf)

    processedDf
  }
}
