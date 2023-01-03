package ingestion.database.partition;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlToDatasetWithPartitionApp {
    public static void main(String[] args) {
        MySqlToDatasetWithPartitionApp app = new MySqlToDatasetWithPartitionApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe jsing JDBC with partitioning")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSSL", "false");
        props.put("serverTimezone", "EST");

        // Used for partitioning
        props.put("partitionColumn", "film_id");  // column to partition on
        props.put("lowerBound", "1");  // Lower bound of the stride
        props.put("upperBound", "1000");  // Upper bound of the stride
        props.put("numPartitions", "10");  // Number of partitions

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "film",
                props);

        df.show(5);
        System.out.println("The dataframe contains " + df.count() + " record(s).");
        System.out.println("The dataframe is split over " + df.rdd()
                .getPartitions().length + " partition(s).");
    }
}
