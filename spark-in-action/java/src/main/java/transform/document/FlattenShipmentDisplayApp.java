package transform.document;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class FlattenShipmentDisplayApp {
    public static void main(String[] args) {
        FlattenShipmentDisplayApp app = new FlattenShipmentDisplayApp();
        app.start();
    }

    private void start() {
        // Create a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Flattening JSON document")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", "true")
                .load("data/ch13/json/shipment.json");

        df = df
                .withColumn("supplier_name", df.col("supplier.name"))
                .withColumn("supplier_city", df.col("supplier.city"))
                .withColumn("supplier_state", df.col("supplier.state"))
                .withColumn("supplier_country", df.col("supplier.country"))
                .withColumn("supplier_name", df.col("supplier.name"))
                .drop("supplier")
                .withColumn("customer_name", df.col("customer.name"))
                .withColumn("customer_city", df.col("customer.city"))
                .withColumn("customer_state", df.col("customer.state"))
                .withColumn("customer_country", df.col("customer.country"))
                .drop("customer")
                .withColumn("items", explode(df.col("books")));

        df = df
                .withColumn("qty", df.col("items.qty"))
                .withColumn("title", df.col("items.title"))
                .drop("items")
                .drop("books");

        df.show(5, false);
        df.printSchema();

        df.createOrReplaceTempView("shipment_detail");
        Dataset<Row> bookCountDf =
                spark.sql("SELECT COUNT(*) AS bookCount FROM shipment_detail");
        bookCountDf.show(false);
    }
}
