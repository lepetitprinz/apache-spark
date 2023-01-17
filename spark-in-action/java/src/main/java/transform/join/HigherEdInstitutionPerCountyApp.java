package transform.join;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.*;

public class HigherEdInstitutionPerCountyApp {
    public static void main(String[] args) {
        HigherEdInstitutionPerCountyApp app = new HigherEdInstitutionPerCountyApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Join")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

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

        System.out.println("Census data");
        censusDf.sample(0.1).show(3, false);
        censusDf.printSchema();

        // Higher education institution
        Dataset<Row> higherEduDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/ch12/dapip/InstitutionCampus.csv");

        higherEduDf = higherEduDf
                .filter("LocationType = 'Institution'")
                .withColumn(
                        "addressElements",
                        split(higherEduDf.col("Address"), " "));

        higherEduDf = higherEduDf
                .withColumn(
                        "addressElementCount",
                        size(higherEduDf.col("addressElements")));

        higherEduDf = higherEduDf
                .withColumn(
                        "zip9",
                        element_at(  // Takes the last element in the array
                                higherEduDf.col("addressElements"),
                                higherEduDf.col("addressElementCount")));

        higherEduDf = higherEduDf
                .withColumn("splitZipCode",
                        split(higherEduDf.col("zip9"), "-"));

        higherEduDf = higherEduDf
                .withColumn("zip", higherEduDf.col("splitZipCode").getItem(0))
                .withColumnRenamed("LocationName", "location")
                .drop("DapipId", "OpeId", "ParentName", "ParentDapipId")
                .drop("LocationType", "Address", "GeneralPhone", "AdminName")
                .drop("AdminPhone", "AdminEmail", "Fax", "UpdateDate", "zip9")
                .drop("addressElements", "addressElementCount", "splitZipCode");

        System.out.println("Higher education institutions (DAPIP)");
        higherEduDf.sample(0.1).show(3, false);
        higherEduDf.printSchema();

        // Zip to county
        Dataset<Row> countyZipDf = spark
                .read()
                .format("csv")
                .option("header", "true")
                .option("InferSchema", "true")
                .load("data/ch12/hud/COUNTY_ZIP_092018.csv");

        countyZipDf = countyZipDf
                .drop("res_ratio", "bus_ratio", "oth_ratio", "tot_ratio");

        System.out.println("Counties / ZIP Codes (HUD)");
        countyZipDf.sample(0.1).show(3, false);
        countyZipDf.printSchema();

        // Institutions per county id
        Dataset<Row> institPerCountyDf = higherEduDf.join(
                countyZipDf,
                higherEduDf.col("zip").equalTo(countyZipDf.col("zip")),
                "inner");

        System.out.println("Higher education institutions left-joined with HUD");
        institPerCountyDf
                .filter(higherEduDf.col("zip").equalTo(27517))
                .show(20, false);
        institPerCountyDf.printSchema();

        // Dropping all zip columns
        System.out.println("Attempt to drop the zip column");
        institPerCountyDf.drop("zip").sample(0.1).show(3, false);

        // Dropping the zip column inherited from the higher edu dataframe
        System.out.println("Attempt to drop the zip column");
        institPerCountyDf.drop(higherEduDf.col("zip")).sample(0.1).show(3, false);

        // Institutions per county name
        institPerCountyDf = institPerCountyDf.join(
                censusDf,
                institPerCountyDf.col("county").equalTo(censusDf.col("countyId")),
                "left");

        // Final clean up
        institPerCountyDf = institPerCountyDf
                .drop(higherEduDf.col("zip"))
                .drop(countyZipDf.col("county"))
                .drop("countyId")
                .distinct();  // remove duplicate rows

        System.out.println("Higher education institutions in ZIP Code 27517 (NC)");
        institPerCountyDf
                .filter(higherEduDf.col("zip").equalTo(27517))
                .show(20, false);

        System.out.println("Higher education institutions in ZIP Code 02138 (MA)");
        institPerCountyDf
                .filter(higherEduDf.col("zip").equalTo(2138))
                .show(20, false);

        System.out.println("Institutions with improper counties");
        institPerCountyDf
                .filter("county is null")
                .show(200, false);

        System.out.println("Final list");
        institPerCountyDf.show(200, false);
        System.out.println("The combined list has " + institPerCountyDf.count()
                + " elements.");

        Dataset<Row> aggDf = institPerCountyDf
                .groupBy("county", "pop2017")
                .count();
        aggDf = aggDf.orderBy(aggDf.col("count").desc());
        aggDf.show(25, false);

        Dataset<Row> popDf = aggDf
                .filter("pop2017>30000")
                .withColumn("institutionPer10k", expr("count*10000/pop2017"));
        popDf = popDf.orderBy(popDf.col("institutionPer10k").desc());
        popDf.show(25, false);
    }
}
