package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.split;

public class JsonIngestionSchemaManipulation {
    public static void main(String[] args) {
        JsonIngestionSchemaManipulation app = new JsonIngestionSchemaManipulation();
        app.start();
    }

    private void start() {
        // Create a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("")
                .master("local[*]")
                .getOrCreate();

        // Read a JSON file
        Dataset<Row> df = spark.read().format("json")
                .load("/Users/yjkim-studio/data/spark/Restaurants_in_Durham_County_NC.json");

        System.out.println("*** Right after ingestion");
        df.show(5);
        df.printSchema();
        System.out.println("Records: " + df.count());

        df = df.withColumn("county", lit("Durham"))
            .withColumn("datasetId", df.col("fields.id"))
            .withColumn("name", df.col("fields.premise_name"))
            .withColumn("address1", df.col("fields.premise_address1"))
            .withColumn("address2", df.col("fields.premise_address2"))
            .withColumn("city", df.col("fields.premise_city"))
            .withColumn("state", df.col("fields.premise_state"))
            .withColumn("zip", df.col("fields.premise_zip"))
            .withColumn("tel", df.col("fields.premise_phone"))
            .withColumn("dateStart", df.col("fields.opening_date"))
            .withColumn("dateEnd", df.col("fields.closing_date"))
            .withColumn("type", // <id> - <label> notation -> get label
                split(df.col("fields.type_description"), " - ").getItem(1))
            .withColumn("geoX", df.col("fields.geolocation").getItem(0))
            .withColumn("geoY", df.col("fields.geolocation").getItem(1));

        df = df.withColumn("id",
                concat(df.col("state"), lit("_"), df.col("county"),
                        lit("_"), df.col("datasetId")));

        System.out.println("*** Dataframe transformed");
        df.show(5);
        df.printSchema();
    }
}
