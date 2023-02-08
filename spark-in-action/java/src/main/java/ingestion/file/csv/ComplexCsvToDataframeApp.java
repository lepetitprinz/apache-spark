package ingestion.file.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ComplexCsvToDataframeApp {

    public static void main(String[] args) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }

    private void start() {
        // Create a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Complex csv to Dataframe")
                .master("local")
                .getOrCreate();
        System.out.println("Using Apache Spark v" + spark.version());

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a csv file with header, stores it in a dataframe
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")  // Separator between values is a semicolon
                .option("quote", "")  // Quote character is a star(*)
                .option("dateFormat", "MM/dd/yyyy")  // Date format matches the month/day/year format
                .option("inferSchema", true)  // Spark will infer the schema
                .load("data/ch07/books.csv");

        System.out.println("Excerpt of the dataframe content:");

        // Shows at most 7 rows form the dataframe, with columns as wide as 70 characters
        df.show(7, 70);
        System.out.println("Dataframe's schema:");
        df.printSchema();
    }
}
