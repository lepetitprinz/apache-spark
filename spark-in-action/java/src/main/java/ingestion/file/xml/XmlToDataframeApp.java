package ingestion.file.xml;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class XmlToDataframeApp {
    public static void main(String[] args) {
        XmlToDataframeApp app = new XmlToDataframeApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("XML to dataframe")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a XML file with header
        Dataset<Row> df = spark.read().format("xml")
                .option("rowTag", "row")  // Element or tag that indicates a record in the XML file
                .load("data/ch07/nasa-patents.xml");

        df.show(5);
        df.printSchema();
    }
}
