package transform.udf.open;

import static org.apache.spark.sql.functions.callUDF;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.to_timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.List;

public class OpenedLibrariesApp {
    public static void main(String[] args) {
        OpenedLibrariesApp app = new OpenedLibrariesApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Custom UDF to check if in range")
                .master("local[*]")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Register UDF
        spark
                .udf()  // Accesses the UDFs
                .register(
                        "isOpen",  // Name of the function as used in calls
                        new IsOpenUdf(), // Class implementing the user-defined function
                        DataTypes.BooleanType);  // Return type that will be applied to the new column

        Dataset<Row> librariesDf = spark.read()
                .format("csv")
                .option("header", "true")
                .option("inferSchema", "true")
                .option("encoding", "cp1252")
                .load("data/ch14/south_dublin_libraries/sdlibraries.csv")


        librariesDf.show(false);
        librariesDf.printSchema();

        Dataset<Row> dateTimeDf = createDataframe(spark);
        dateTimeDf.show(false);
        dateTimeDf.printSchema();

        Dataset<Row> df = librariesDf.crossJoin(dateTimeDf);
        df.show(false);

        // Using the dataframe API
        Dataset<Row> finalDf = df.withColumn(
                        "open",
                        callUDF(
                                "isOpen",
                                col("Opening_Hours_Monday"),
                                col("Opening_Hours_Tuesday"),
                                col("Opening_Hours_Wednesday"),
                                col("Opening_Hours_Thursday"),
                                col("Opening_Hours_Friday"),
                                col("Opening_Hours_Saturday"),
                                lit("Closed"),
                                col("date")))
                .drop("Opening_Hours_Monday")
                .drop("Opening_Hours_Tuesday")
                .drop("Opening_Hours_Wednesday")
                .drop("Opening_Hours_Thursday")
                .drop("Opening_Hours_Friday")
                .drop("Opening_Hours_Saturday");
        finalDf.show();
    }

    private static Dataset<Row> createDataframe(SparkSession spark) {
        // Creates a schema for the initial data
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "date_str",
                        DataTypes.StringType,
                        false) });

        List<Row> rows = new ArrayList<>();
        rows.add(RowFactory.create("2019-03-11 14:30:00"));
        rows.add(RowFactory.create("2019-04-27 16:00:00"));
        rows.add(RowFactory.create("2020-01-26 05:00:00"));

        return spark
                .createDataFrame(rows, schema)
                .withColumn("date", to_timestamp(col("date_str")))
                .drop("date_str");
    }
}
