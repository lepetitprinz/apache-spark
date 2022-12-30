package ingestion.orc;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class OrcToDataframeApp {
    public static void main(String[] args) {
        OrcToDataframeApp app = new OrcToDataframeApp();
        app.start();
    }

    /**
     * The processing code.
     */
    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("ORC to Dataframe")
                .config("spark.sql.orc.impl", "native")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads an ORC file, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("orc")
                .load("data/ch07/demo-11-zlib.orc");

        df.show(10);
        df.printSchema();
        System.out.println("The dataframe has " + df.count() + " rows.");
    }
}
