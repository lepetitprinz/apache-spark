package ingestion.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlToDatabaseWithLongUrlApp {

    public static void main(String[] args) {
        MySqlToDatabaseWithLongUrlApp app = new MySqlToDatabaseWithLongUrlApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Using a JDBC URL
        String jdbcUrl = "jdbc:mysql://localhost:3306/sakila"
                + "?user=root"
                + "&password=mysql"
                + "&useSSL=false"
                + "&serverTimezone=EST";

        Dataset<Row> df = spark.read()
                .jdbc(jdbcUrl, "actor", new Properties());

        df = df.orderBy(df.col("last_name"));

        // Display the dataframe and some of its metadata
        df.show();
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " records");

        spark.stop();
    }
}
