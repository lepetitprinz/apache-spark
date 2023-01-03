package ingestion.database.dialect;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.jdbc.JdbcDialect;
import org.apache.spark.sql.jdbc.JdbcDialects;

public class InformixToDatasetApp {

    public static void main(String[] args) {
        InformixToDatasetApp app = new InformixToDatasetApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Informix to Dataframe using a JDBC Connection")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Specific informix dialect
        JdbcDialect dialect = new InformixJdbcDialect();
        JdbcDialects.registerDialect(dialect);

        Dataset<Row> df = spark.read()
                .format("jdbc")
                .option(
                        "url",
                        "jdbc:informix-sqli://[::1]:33378/stores_demo:IFXHOST=lo_informix1210;DELIMIDENT=Y")
                .option("dbtable", "customer")
                .option("user", "informix")
                .option("password", "informix")
                .load();

        df.show(5);
        df.printSchema();
        System.out.println("the dataframe contains " + df.count() + " records");

        spark.stop();
    }
}
