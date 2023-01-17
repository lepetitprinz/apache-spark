package transform.join;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * how to use adaptive query execution (AQE)
 */
public class HigherEdInstitutionPerCountyUsingAqeApp {
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyUsingAqeApp app =
                new HigherEdInstitutionPerCountyUsingAqeApp();
        boolean useAqe = true;
        app.start(useAqe);
        app.start(useAqe);
        app.start(useAqe);
        app.start(useAqe);
        app.start(useAqe);
    }

    private void start(boolean useAqe) {
        SparkSession spark = SparkSession.builder()
                .appName("Join using AQE")
                .master("local[*]")
                .config("spark.sql.adaptive.enabled", useAqe)
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Ingestion of the census data
        Dataset<Row> censusDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/ch12/census/PEP_2017_PEPANNRES.csv");

        censusDf = censusDf
                .drop("GEO.id", "rescen42010", "resbase42010")
                .drop("respop72010", "respop72011", "respop72012", "respop72013")
                .drop("respop72014", "respop72015", "respop72016")
                .withColumnRenamed("respop72017", "pop2017")
                .withColumnRenamed("GEO.id2", "countyId")
                .withColumnRenamed("GEO.display-label", "county");

        // Higher education institution
        Dataset<Row> higherEduDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/ch12/dapip/InstitutionCampus.csv");

        // Filter "location type: Institution" and split "address" column
        higherEduDf = higherEduDf
                .filter("LocationType = 'Institution'")
                .withColumn(
                        "addressElements",
                        split(higherEduDf.col("Address"), " "));

        // add address length column
        higherEduDf = higherEduDf
                .withColumn(
                        "addressElementCount",
                        size(higherEduDf.col("addressElements")));

        // Takes the last elements in array
        higherEduDf = higherEduDf
                .withColumn(
                        "zip9",
                        element_at(
                                higherEduDf.col("addressElements"),
                                higherEduDf.col("addressElementCount")));

        // split "zip code" column
        higherEduDf = higherEduDf
                .withColumn(
                        "splitZipCode",
                        split(higherEduDf.col("zip9"), "-"));

        //
        higherEduDf = higherEduDf
                .withColumn("zip", higherEduDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location");

        higherEduDf = higherEduDf
                .drop("DapipId", "OpeId", "ParentName", "ParentDapipId")
                .drop("LocationType", "Address", "GeneralPhone", "AdminName")
                .drop("AdminPhone", "AdminEmail", "Fax", "UpdateDate", "zip9")
                .drop("addressElements", "addressElementCount", "splitZipCode");

        // Zip to county
        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/ch12/hud/COUNTY_ZIP_092018.csv");

        countyZipDf = countyZipDf
                .drop("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio");

        long t0 = System.currentTimeMillis();

        Dataset<Row> institPerCountyDf = higherEduDf.join(
                countyZipDf,
                higherEduDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(
                censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");

        // Action
        institPerCountyDf = institPerCountyDf.cache();
        institPerCountyDf.collect();

        long t1 = System.currentTimeMillis();
        System.out.println("AQE is " + useAqe + ", join operations took: " + (t1 - t0) + " ms.");

        spark.stop();
    }
}
