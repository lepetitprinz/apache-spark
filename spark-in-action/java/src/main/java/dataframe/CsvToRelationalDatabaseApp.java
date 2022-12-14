package dataframe;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.lit;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class CsvToRelationalDatabaseApp {
    public static void main(String[] args) {
        CsvToRelationalDatabaseApp app = new CsvToRelationalDatabaseApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("CSV to DB")
                .master("local")
                .getOrCreate();

        // Reads a csv file with header, called authors.csv, and stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/authors.csv");

        // Create a new column called "name" as the concatenation of lname,
        // a virtual column containing ", " and the fname column
        df = df.withColumn(
    "name",
            concat(
                df.col("lname"),
                lit(", "),
                df.col("fname")
            )
        );

        String dbConnectionUrl = "jdbc:postgresql://localhost/spark";

        // Properties to connect to the database
        Properties prop = new Properties();
        prop.setProperty("driver", "org.postgresql.Driver");
        prop.setProperty("user", "spark");
        prop.setProperty("password", "spark");
        df.write()
                .mode(SaveMode.Overwrite)
                .jdbc(dbConnectionUrl, "spark_in_action_ch02", prop);

        System.out.println("Process complete");
    }
}
