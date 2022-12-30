package ingestion.json;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MultilineJsonToDataframeApp {

    public static void main(String[] args) {
        MultilineJsonToDataframeApp app = new MultilineJsonToDataframeApp();
        app.start();
    }

    private void start()  {
        // Creates a session on local master
        SparkSession spark = SparkSession.builder()
                .appName("Multiline JSON to Dataframe")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/ch07/durham-nc-foreclosure-2006-2016.json");

        df.show(3);
        df.printSchema();
    }
}
