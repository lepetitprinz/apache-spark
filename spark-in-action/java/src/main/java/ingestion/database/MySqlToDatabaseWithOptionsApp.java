package ingestion.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class MySqlToDatabaseWithOptionsApp {

    public static void main(String[] args) {
        MySqlToDatabaseWithOptionsApp app = new MySqlToDatabaseWithOptionsApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Database with Options using a JDBC Connection")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option("url", "jdbc:mysql://localhost:3306/sakila")
                .option("dbtable", "actor")
                .option("user", "root")
                .option("password", "mysql")
                .option("useSSL", "false")
                .option("serverTimezone", "EST")
                .load();

        df = df.orderBy(df.col("last_name"));

        // Displays the dataframe and some of its metadata
        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " records");

        spark.stop();
    }
}
