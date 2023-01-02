package ingestion.database;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlWithWhereClauseToDatasetApp {

    public static void main(String[] args) {
        MySqlWithWhereClauseToDatasetApp app = new MySqlWithWhereClauseToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with where clause to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSSL", "false");
        props.put("serverTimezone", "EST");

        String sqlQuery = "select * from film where "
                + "("
    }
}
