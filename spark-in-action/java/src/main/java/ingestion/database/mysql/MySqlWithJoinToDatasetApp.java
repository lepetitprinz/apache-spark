package ingestion.database.mysql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.Properties;

public class MySqlWithJoinToDatasetApp {

    public static void main(String[] args) {
        MySqlWithJoinToDatasetApp app = new MySqlWithJoinToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("MySQL with join to Dataframe using JDBC")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Using properties
        Properties props = new Properties();
        props.put("user", "root");
        props.put("password", "mysql");
        props.put("useSLL", "false");
        props.put("allowPublicKeyRetrieval", "true");
        props.put("serverTimezone", "EST");

        // Builds the SQL query doing the join operation
        String sqlQuery =
                "select actor.first_name, actor.last_name, film.title, film.description "
                + "from actor, film_actor, film "
                + "where actor.actor_id = film_actor.actor_id "
                + "and film_actor.film_id = film.film_id";

        Dataset<Row> df = spark.read().jdbc(
                "jdbc:mysql://localhost:3306/sakila",
                "(" + sqlQuery + ") actor_film_alias",
                props);

        df.show(5);
        df.printSchema();
        System.out.println("The dataframe contains " + df.count() + " records");
    }
}
