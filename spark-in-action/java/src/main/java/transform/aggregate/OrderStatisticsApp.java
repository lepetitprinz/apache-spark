package transform.aggregate;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.avg;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OrderStatisticsApp {
    private static Logger log = LoggerFactory.getLogger(OrderStatisticsApp.class);

    public static void main(String[] args) {
        OrderStatisticsApp app = new OrderStatisticsApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Data Aggregation")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a csv file with header
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .load("data/ch15/orders/orders.csv");

        // Calculating the orders info using the dataframe API
        Dataset<Row> apiDf = df
                .groupBy(col("firstName"), col("lastName"), col("state"))
                .agg(sum("quantity"), sum("revenue"), avg("revenue"));
        apiDf.show(20);

        // Calculating the orders info using SparkSQL
        df.createOrReplaceTempView("orders");
        String sqlStatement =
                "SELECT firstName, lastName, state, " +
                "SUM(quantity), SUM(revenue), AVG(revenue) " +
                "FROM orders " +
                "GROUP BY firstName, lastName, state";  // Spark does not allow parentheses around the GROUP BY part

        Dataset<Row> sqlDf = spark.sql(sqlStatement);
        sqlDf.show(20);
    }
}
