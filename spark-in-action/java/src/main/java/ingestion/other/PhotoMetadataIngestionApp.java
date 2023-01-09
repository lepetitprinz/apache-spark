package ingestion.other;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class PhotoMetadataIngestionApp {
    public static void main(String[] args) {
        PhotoMetadataIngestionApp app = new PhotoMetadataIngestionApp();
        app.start();
    }

    private boolean start() {
        SparkSession spark = SparkSession.builder()
                .appName("EXIF to Dataset")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String importDirectory = "data/ch09";

        Dataset<Row> df = spark.read()
                .format("exif")
                .option("recursive", "true")  // be able to read recursively through the directories
                .option("limit", "100000")  // limit files
                .option("extensions", "jpg, jpeg")
                .load(importDirectory);

        System.out.println("Import " + df.count() + " photos");
        df.printSchema();
        df.show(5);

        return true;
    }
}
