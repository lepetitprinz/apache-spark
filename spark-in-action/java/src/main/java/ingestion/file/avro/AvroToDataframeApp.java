package ingestion.file.avro;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class AvroToDataframeApp {
    public static void main(String[] args) {
        AvroToDataframeApp app = new AvroToDataframeApp();
        app.start();
    }

    private void start () {
        SparkSession spark = SparkSession.builder()
                .appName("Avro to Dataframe")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("avro")
                .load("data/ch07/weather.avro");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows");
    }
}
