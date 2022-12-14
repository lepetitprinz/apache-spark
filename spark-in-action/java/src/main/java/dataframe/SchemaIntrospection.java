package dataframe;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;

public class SchemaIntrospection {
    public static void main(String[] args) {
        SchemaIntrospection app = new SchemaIntrospection();
        app.start();
    }

    private void start() {

        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Schema instropection")
                .master("local")
                .getOrCreate();

        // Reads a CSV file with header
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("/Users/yjkim-studio/data/spark/Restaurants_in_Wake_County.csv");

        // Let's transform our dataframe
        df = df.withColumn("county", lit("Wake"))
                .withColumnRenamed("HSISID", "datasetId")
                .withColumnRenamed("NAME", "name")
                .withColumnRenamed("ADDRESS1", "address1")
                .withColumnRenamed("ADDRESS2", "address2")
                .withColumnRenamed("CITY", "city")
                .withColumnRenamed("STATE", "state")
                .withColumnRenamed("POSTALCODE", "zip")
                .withColumnRenamed("PHONENUMBER", "tel")
                .withColumnRenamed("RESTAURANTOPENDATE", "dateStart")
                .withColumnRenamed("FACILITYTYPE", "type")
                .withColumnRenamed("X", "geoX")
                .withColumnRenamed("Y", "geoY");

        df = df.withColumn("id", concat(
                df.col("state"),
                lit("_"),
                df.col("county"), lit("_"),
                df.col("datasetId")));

        StructType schema = df.schema();  // Extracts the schema

        System.out.println("*** Schema as a tree");
        schema.printTreeString();  // Displays the schema as a tree
        String schemaAsString = schema.mkString();  // Extracts the schema as a string
        System.out.println("*** Schema as string: " + schemaAsString);
        String schemaAsJson = schema.prettyJson();  // Extracts the schema as a JSON object in a string
        System.out.println("*** Schema as JSON: " + schemaAsJson);
    }
}
