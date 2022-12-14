package dataframe;

import org.apache.spark.Partition;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;

public class CsvIngestionSchemaManipulation {
    public static void main(String[] args) {
        CsvIngestionSchemaManipulation app = new CsvIngestionSchemaManipulation();
        app.start();
    }

    private void start () {
        // Creates a Spark Session
        SparkSession spark = SparkSession.builder()
                .appName("Restaurants in Wake Country, NC")
                .master("local")
                .getOrCreate();

        // Creates a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")  // csv file has a header row
                .load("/Users/yjkim-studio/data/spark/Restaurants_in_Wake_County.csv");

        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();
        System.out.println("Total records: " + df.count());

        // Rename columns
        df = df.withColumn("county", lit("Wake"))
            .withColumnRenamed("HSISID", "datasetId")
            .withColumnRenamed("Name", "name")
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
            .drop("OBJECTID")
            .drop("PERMITID")
            .drop("GEOCODESTATUS");

        //
        df = df.withColumn("id", concat (
                df.col("state"), lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();

        // Partition
        System.out.println("*** Looking at partitions");
        Partition[] partitions = df.rdd().partitions();
        int partitionCount = partitions.length;
        System.out.println("Partition count before repartition: " + partitionCount);

        df = df.repartition(4);
        System.out.println("partition count after repartition: " + df.rdd().partitions().length);


    }
}
