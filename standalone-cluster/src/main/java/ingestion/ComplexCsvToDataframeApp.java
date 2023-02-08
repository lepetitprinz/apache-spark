package ingestion;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class ComplexCsvToDataframeApp {
    private static final String DIR = "/opt/spark-data/spark-in-action-data/";
    private static final String INPUT_PATH = DIR + "input/ch07/books.csv";
    private static final String OUTPUT_PATH = DIR + "output/ch07/books.csv";

    public static void main(String[] args) {
        ComplexCsvToDataframeApp app = new ComplexCsvToDataframeApp();
        app.start();
    }

    private void start() {
        // create a session on a master
        SparkSession spark = SparkSession.builder()
            .appName("Complex csv to Dataframe")
            .master("spark://spark-master:7077")
            .config("spark.driver.memory", "2g")
            .config("spark.executor.memory", "2g")
            .getOrCreate();

        // Reads a csv file with header
        Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .option("multiline", true)
            .option("sep", ";")  // Separator between values is a semicolon
            .option("quote", "")  // Quote character is a star(*)
            .option("dateFormat", "MM/dd/yyyy")  // Date format matches the month/day/year format
            .option("inferSchema", true)  // Spark will infer the schema
            .load(INPUT_PATH);

        df.write().format("csv")
            .option("header", true)
            .mode(SaveMode.Overwrite)
            .save(OUTPUT_PATH);

        spark.stop();
    }
}
