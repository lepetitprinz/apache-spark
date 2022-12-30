package ingestion.text;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class TextToDataframeApp {

    public static void main(String[] args) {
        TextToDataframeApp app = new TextToDataframeApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Text to Dataframe")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a text file, stores it in a dataframe
        Dataset<Row> df = spark.read().format("text")
                .load("data/ch07/romeo-juliet-pg1777.txt");

        df.show(10);
        df.printSchema();
    }
}
