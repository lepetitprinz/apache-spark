package ingestion.database.mysql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
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

        spark.sparkContext().setLogLevel("ERROR");

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSSL", "false");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("serverTimezone", "EST");

        String sqlQuery = "select * from film where "
                + "(title like \"%ALIEN%\" or title like \"%victory%\" "
                + "or title like \"%agent%\" or description like \"%action%\") "
                + "and rental_rate>1 "
                + "and (rating=\"G\" or rating=\"PG\")";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sqlQuery + ") film_alias",  // the SQL query is between parentheses
                props);

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " records");
    }
}
