package ingestion.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class ComplexCsvToDataframePushDownFilterApp {
    public static void main(String[] args) {
        ComplexCsvToDataframePushDownFilterApp app = new ComplexCsvToDataframePushDownFilterApp();
        app.start();
    }

    private void start() {
        // Create a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Complex csv to dataframe with filter")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("quote", "*")
                .option("dateFormat", "MM/dd/yyyy")
                .option("inferSchema", true)
                .load("data/ch07/books.csv")
                .filter("authorId = 1");

        System.out.println("Excerpt of the dataframe content:");

        df.show(7, 70);
        System.out.println("Dataframe's schema:");
        df.printSchema();
    }
}
