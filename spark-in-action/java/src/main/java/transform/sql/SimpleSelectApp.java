package transform.sql;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Simple SQL select on ingested data
 */

public class SimpleSelectApp {
    public static void main(String[] args) {
        SimpleSelectApp app = new SimpleSelectApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Creates a schema
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        // Reads a csv file with header, stores it in a dataframe
        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/ch11/populationbycountry19802010millions.csv");
        df.createOrReplaceTempView("geodata");  // Creates a session-scoped temporary view
        df.printSchema();

        Dataset<Row> smallCountires =
                spark.sql(  // Executes the query
                        "SELECT * FROM geodata WHERE yr1980 < 1 ORDER BY 2 LIMIT 5");

        smallCountires.show(10, false);
    }
}
