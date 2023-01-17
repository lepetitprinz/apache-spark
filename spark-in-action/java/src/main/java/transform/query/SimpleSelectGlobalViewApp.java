package transform.query;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class SimpleSelectGlobalViewApp {
    public static void main(String[] args) {
        SimpleSelectGlobalViewApp app = new SimpleSelectGlobalViewApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Simple SELECT using SQL")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField("geo", DataTypes.StringType, true),
                DataTypes.createStructField("yr1980", DataTypes.DoubleType, false)
        });

        Dataset<Row> df = spark.read()
                .format("csv")
                .option("header", true)
                .schema(schema)
                .load("data/ch11/populationbycountry19802010millions.csv");

        df.createOrReplaceGlobalTempView("geodata");
        df.printSchema();

        Dataset<Row> smallCountriesDf =
                spark.sql(
                        "SELECT * FROM global_temp.geodata "
                        + "WHERE yr1980 < 1 ORDER BY 2 LIMIT 5"
                );

        smallCountriesDf.show(10, false);

        // Create a new session and query the same data
        SparkSession spark2 = spark.newSession();
        Dataset<Row> slightlyBiggerCountriesDf =
                spark2.sql(
                        "SELECT * FROM global_temp.geodata "
                        + "WHERE yr1980 >= 1 ORDER BY 2 LIMIT 5");
        slightlyBiggerCountriesDf.show(10, false);
    }
}
