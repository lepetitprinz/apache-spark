package ingestion.database;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlToDatasetWithEnvVariableApp {

    public static void main(String[] args) {
        MySqlToDatasetWithEnvVariableApp app = new MySqlToDatasetWithEnvVariableApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL to Dataset with env variable")
                .master("local")
                .getOrCreate();

        Properties props = new Properties();
        props.put("user", "root");

        byte [] password = System.getenv("DB_PASSWORD").getBytes();
        props.put("password", new String(password));
        password = null;
        props.put("useSSL", "false");

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila?serverTimezone=EST",
                "actor", props);

        df = df.orderBy(df.col("last_name"));

        df.show(5);
        df.printSchema();
        System.out.println("the dataframe contains " + df.count() + " records");
    }
}
