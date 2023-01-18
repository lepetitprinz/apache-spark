package transform.join

import org.apache.spark.sql.{SparkSession, functions => F}

object HigherEdInstitutionPerCountyApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Join Application")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    var censusDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .option("encoding", "cp1252")
      .load("data/ch12/census/PEP_2017_PEPANNRES.csv")

    censusDf = censusDf
      .drop("GEO.id", "rescen42010", "resbase42010", "respop72010", "respop72011",
        "respop72012", "respop72013", "respop72014", "respop72015", "respop72016")
      .withColumnRenamed("respop72017", "pop2017")
      .withColumnRenamed("GEO.id2", "countyId")
      .withColumnRenamed("GEO.display-label", "county")

    println("Census data")
    censusDf.sample(0.1).show(3, false)
    censusDf.printSchema()

    // Higher education institution
    var higherEduDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/ch12/dapip/InstitutionCampus.csv")

    higherEduDf = higherEduDf
      .filter("LocationType = 'Institution'")
      .withColumn("addressElements", F.split(F.col("Address"), " "))

    higherEduDf = higherEduDf
      .withColumn("addressElementCount", F.size(F.col("addressElements")))

    higherEduDf = higherEduDf
      .withColumn("zip9",
        F.element_at(
          F.col("addressElements"),
          F.col("addressElementCount"))
      )

    higherEduDf = higherEduDf
      .withColumn("splitZipCode", F.split(F.col("zip9"), "-"))

    higherEduDf = higherEduDf
      .withColumn("zip", F.col("splitZipCode").getItem(0))
      .withColumnRenamed("LocationName", "location")
      .drop("DapipId", "OpeId", "ParentName", "ParentDapipId", "LocationType",
      "Address", "GeneralPhone", "AdminName", "AdminPhone", "AdminEmail", "Fax", "UpdateDate",
      "zip9", "addressElements", "addressElementCount", "splitZipCode")

    println("Higher education institutions (DAPIP)")
    higherEduDf.sample(0.1).show(3, false)
    higherEduDf.printSchema()

    // Zip to county
    var countyZipDf = spark.read
      .format("csv")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("data/ch12/hud/COUNTY_ZIP_092018.csv")

    countyZipDf = countyZipDf
      .drop("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio")

    println("Counties/ ZIP codes (HUD)")
    countyZipDf.sample(0.1).show(3, false)
    countyZipDf.printSchema()

    val institPerCountyJoinCondition = higherEduDf.col("zip") === countyZipDf.col("zip")
    // Institutions per county id
    var institPerCountyDf = higherEduDf
      .join(countyZipDf, institPerCountyJoinCondition, "inner")
      .drop(countyZipDf.col("zip"))

    println("Higher education institutions left-joined with HUD")
    institPerCountyDf.filter(F.col("zip") === 27517).show(20, false)

    println("Attempt to drop the zip column")
    institPerCountyDf
      .drop("zip")
      .sample(0.1)
      .show(3, false)

    // Dropping the zip column inherited from the higher ed dataframe
    println("Attempt to drop the zip column")
    institPerCountyDf
      .drop(F.col("zip"))
      .sample(0.1)
      .show(3, false)

    val institPerCountyCondition = institPerCountyDf.col("county") === censusDf.col("countyId")

    // Institutions per county name
    institPerCountyDf = institPerCountyDf
      .join(censusDf, institPerCountyCondition, "left")
      .drop(censusDf.col("county"))

    // Final clean up
    institPerCountyDf = institPerCountyDf
      .drop(F.col("zip"))
      .drop(F.col("county"))
      .drop("countyId")
      .distinct

    println("Higher education institutions in ZIP Code 27517 (NC)")
    institPerCountyDf.filter(F.col("zip") === 27517).show(20, false)

    println("Higher education institutions in ZIP Code 02138 (MA)")
    institPerCountyDf.filter(higherEduDf.col("zip") === 2138).show(20, false)

    println("Institutions with improper counties")
    institPerCountyDf.filter("county is null").show(200, false)

    println("Final list")
    institPerCountyDf.show(200, false)
    println("The combined list has " + institPerCountyDf.count + " elements.")

//    var aggDf = institPerCountyDf.groupBy("county", "pop2017").count
//    aggDf = aggDf.orderBy(aggDf.col("count").desc)
//    aggDf.show(25, false)
//
//    var popDf = aggDf.filter("pop2017>30000")
//      .withColumn("institutionPer10k", F.expr("count*10000/pop2017"))
//
//    popDf = popDf.orderBy(popDf.col("institutionPer10k").desc)
//    popDf.show(25, false)

    spark.stop
  }
}
