package transform.document

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession, functions => F}
import org.slf4j.LoggerFactory

import java.util

class NestJoinApp {

  private val log = LoggerFactory.getLogger(classOf[NestJoinApp])
  val TEMP_COL = "temp_column"

  def start(): Unit = {
    // creates a session on a local master
    val spark = SparkSession.builder
      .appName("Nest Join two dataset")
      .master("local")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val businessDf = spark.read
      .format("csv")
      .option("header", "true")
      .load("data/ch13/orangecounty_restaurants/businesses.csv")

    val inspectionDf = spark.read
      .format("csv")
      .option("header", "true")
      .load("data/cj13/orangecounty_restaurants/inspections.csv")

    businessDf.show(3)
    businessDf.printSchema

    inspectionDf.show(3)
    inspectionDf.printSchema

    val factSheetDf = nestedJoin(businessDf, inspectionDf,
      "business_id", "business_id", "inner", "inspections")

    factSheetDf.show(3)
    factSheetDf.printSchema

    spark.stop
  }

  def nestedJoin(leftDf: Dataset[Row],
                 rightDf: Dataset[Row],
                 leftJoinCol: String,
                 rightJoinCol: String,
                 joinType: String,
                 nestedCol: String): Dataset[Row] = {
    
    // Performs the join
    var resDf = leftDf.join(
      rightDf,
      rightDf.col(rightJoinCol) === leftDf.col(leftJoinCol),
      joinType)

    // Makes a list of the left columns
    val leftColumns: Array[Column] = getColumns(leftDf)

    if (log.isDebugEnabled) {
      log.debug("columns: {}", leftColumns)
      log.debug("Schema and data:")
      resDf.printSchema
      resDf.show(3)
    }

    // Copies all the columns from the left/master
    val allColumns: Array[Column] = util.Arrays.copyOf(leftColumns, leftColumns.length)

    // Adds a column, which is a structure containing all the columns
    allColumns(leftColumns.length) = F.struct(getColumns(rightDf):_*).alias(TEMP_COL)

    // Performs a select on all columns
    resDf = resDf.select(allColumns:_*)

    if (log.isDebugEnabled) {
      log.debug("Before nested join, {} rows.", resDf.count)
      resDf.printSchema
      resDf.show(3)
    }

    resDf = resDf.groupBy(leftColumns:_*).agg(F.collect_list(F.col(TEMP_COL)).as(nestedCol))

    if (log.isDebugEnabled) {
      resDf.printSchema
      resDf.show(3)
      log.debug("After nested join, {} rows.", resDf.count)
    }

    resDf
  }

  private def getColumns(df: Dataset[Row]): Array[Column] = {
    val fieldnames = df.columns
    val columns = new Array[Column](fieldnames.length)
    var i = 0
    for (fieldname <- fieldnames)
      columns(i) = df.col(fieldname)
      i = i + 1
    columns
  }
  object NestJoinApplication {
    def main(args: Array[String]): Unit = {
      val app = new NestJoinApp
      app.start()
    }
  }
}
