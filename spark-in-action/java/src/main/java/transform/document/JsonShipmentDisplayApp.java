package transform.document;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class JsonShipmentDisplayApp {
    public static void main(String[] args) {
        JsonShipmentDisplayApp app = new JsonShipmentDisplayApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Display json raw data")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a JSON
        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", "true")
                .load("data/ch13/json/shipment.json");

        df.show(5, 16);
        df.printSchema();
    }
}
