package transform.join;

import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.split;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class RecordTransformationApp {
    public static void main(String[] args) {
        RecordTransformationApp app = new RecordTransformationApp();
        app.start();
    }

    private void start() {
        // Creates the session
        SparkSession spark = SparkSession.builder()
                .appName("Record Transformation")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Ingestion of the data
        Dataset<Row> intermediateDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/ch12/census/PEP_2017_PEPANNRES.csv");

        // Renaming and dropping the columns do not need
        intermediateDf = intermediateDf
                .drop("GEO.id")
                .withColumnRenamed("GEO.id2", "id")
                .withColumnRenamed("GEO.display-label", "label")
                .withColumnRenamed("rescen42010", "real2010")
                .drop("resbase42010")
                .withColumnRenamed("respop72010", "est2010")
                .withColumnRenamed("respop72011", "est2011")
                .withColumnRenamed("respop72012", "est2012")
                .withColumnRenamed("respop72013", "est2013")
                .withColumnRenamed("respop72014", "est2014")
                .withColumnRenamed("respop72015", "est2015")
                .withColumnRenamed("respop72016", "est2016")
                .withColumnRenamed("respop72017", "est2017");

        intermediateDf.printSchema();
        intermediateDf.show(5);

        // Creates the additional columns
        intermediateDf = intermediateDf
                .withColumn("countyState", split(intermediateDf.col("label"), ", "))
                .withColumn("stateId", expr("int(id/1000)"))
                .withColumn("countyId", expr("id%1000"));
        intermediateDf.printSchema();
        intermediateDf.sample(.01).show(5, false); // random sample

        intermediateDf = intermediateDf
                .withColumn("state", intermediateDf.col("countyState").getItem(1))
                .withColumn("county", intermediateDf.col("countyState").getItem(0))
                .drop("countyState");
        intermediateDf.printSchema();
        intermediateDf.sample(.01).show(5, false);

        // Performs some statistics on the intermediate dataframe
        Dataset<Row> statDf = intermediateDf
                .withColumn("diff", expr("est2010-real2010"))
                .withColumn("growth", expr("est2017-est2010"))
                .drop("id", "label", "real2010", "est2010", "est2011", "est2012")
                .drop("est2013", "est2014", "est2015", "est2016", "est2017");
        statDf = statDf.sort(statDf.col("growth").desc());
        statDf.printSchema();
        statDf.sample(.01).show(5, false);
    }
}
