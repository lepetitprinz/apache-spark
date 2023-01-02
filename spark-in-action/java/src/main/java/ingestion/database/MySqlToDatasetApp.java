package ingestion.database;

import java.util.Properties;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class MySqlToDatasetApp {

    public static void main(String[] args) {
        MySqlToDatasetApp app = new MySqlToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Using properties object, which is going to be used to collect the properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSLL", "false");  // Custom property (MySQL needs to be told that it won't use SSL

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST",  // JDBC URL
                "actor", props);  // Table & Properties

        df = df.orderBy(df.col("last_name"));

        // Displays the dataframe and some of its metadata
        df.show(5);
        df.printSchema();;
        System.out.println("The dataframe contains " + df.count() + " records");

        spark.stop();
    }
}
