package transform.document

import org.apache.spark.api.java.function.MapFunction
import org.apache.spark.sql.{Encoders, Row, SparkSession, functions => F}

class CsvWithEmbdeddedJsonApp {
  @SerialVersionUID(19712L)
  final private class Jsonifer extends MapFunction[Row, String] {
    @throws[Exception]
    override def call(r: Row) : String = {
      val sb = new StringBuffer
      sb.append("{ \"dept\": \"")
      sb.append(r.getString(0))
      sb.append("\",")

      var s = r.getString(1).toString
      if (s != null) {
        s = s.trim
        if (s.charAt(0) == '{')
          s = s.substring(1, s.length - 1)
      }

      sb.append(s)
      sb.append(", \"location\": \"")
      sb.append(r.getString(2))
      sb.append("\"}")
      sb.toString
    }
  }

  def start(): Unit = {
    // Creates a session on a local master
    val spark = SparkSession.builder
      .appName("Processing embedded json data")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val df = spark.read
      .option("header", "true")
      .option("delimiter", "|")
      .csv("data/ch13/misc/csv_with_embedded_json.csv")

    df.show(5, false)
    df.printSchema

    val ds = df.map(new Jsonifer, Encoders.STRING)
    ds.show(5, false)
    ds.printSchema

    var dfJson = spark.read.json(ds)
    dfJson.show(5, false)
    dfJson.printSchema

    dfJson = dfJson
      .withColumn("emp", F.explode(F.col("employee")))
      .drop("employee")

    dfJson.show(5, false)
    dfJson.printSchema

    dfJson = dfJson
      .withColumn("emp_name",
        F.concat(F.col("emp.name.firstName"), F.lit(" "), F.col("emp.name.lastName")))
      .withColumn("emp_address",
        F.concat(F.col("emp.address.street"), F.lit(" "), F.col("emp.address.unit")))
      .drop("emp")

    dfJson.show(5, false)
    dfJson.printSchema

    spark.stop
  }
}

object CsvWithEmbeddedJsonApplication {
  def main(args: Array[String]): Unit = {
    val app = new CsvWithEmbdeddedJsonApp
    app.start()
  }
}