package transform.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class QueryOnJsonApp {
    public static void main(String[] args) {
        QueryOnJsonApp app = new QueryOnJsonApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Query on a JSON doc")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark.read()
                .format("json")
                .option("multiline", true)
                .load("data/ch12/json/store.json");

        df = df
                .withColumn(
                        "items",
                        functions.explode(df.col("store.book")));

        // Creates a view
        df.createOrReplaceTempView("books");
        Dataset<Row> authorsOfReferenceBookDf =
                spark.sql(
                        "SELECT items.author FROM books "
                        + "WHERE items.category = 'reference'"
                );
        authorsOfReferenceBookDf.show(false);
    }
}
