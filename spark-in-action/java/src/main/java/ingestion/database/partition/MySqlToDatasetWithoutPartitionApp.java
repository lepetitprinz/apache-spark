package ingestion.database.partition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlToDatasetWithoutPartitionApp {

    public static void main(String[] args) {
        MySqlToDatasetWithoutPartitionApp app = new MySqlToDatasetWithoutPartitionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using JDBC without partitioning")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSSL", "flase");
        props.put("serverTimezone", "EST");

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "film", props);

        // Displays the dataframe and some of its metadata
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " records");
        System.out.println("The dataframe is split over " + df.rdd().getPartitions().length
        + " partition(s).");
    }
}
