package ingestion.csv;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType$;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class ComplexCsvToDataframeWithSchemaApp {

    public static final DecimalType$ DecimalType = DecimalType$.MODULE$;
    public static void main(String[] args) {

        ComplexCsvToDataframeWithSchemaApp app = new ComplexCsvToDataframeWithSchemaApp();
        app.start();
    }

    private void start() {
        // Creates a session on local master
        SparkSession spark = SparkSession.builder()
                .appName("Complex csv with a schema to Dataframe")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Creates the schema
        StructType schema = DataTypes.createStructType(new StructField[] {
                DataTypes.createStructField(
                        "id", DataTypes.IntegerType, false),
                DataTypes.createStructField(
                        "authordId", DataTypes.IntegerType, true),
                DataTypes.createStructField(
                        "bookTitle", DataTypes.StringType, false),
                DataTypes.createStructField(
                        "releaseDate", DataTypes.DateType, true), // nullable, but this will be ignore
                DataTypes.createStructField(
                        "url", DataTypes.StringType, false) });

        // SchemaInspector.print(schema);

        // Reads a csv file with header
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .option("multiline", true)
                .option("sep", ";")
                .option("dateFormat", "MM/dd/yyyy")
                .option("quote", "*")
                .schema(schema)
                .load("data/ch07/books.csv");

        // SchemaInspector.print("Schema ...... ", schema);
        // SchemaInspector.print("Dataframe ... ", df);

        df.show(7, 50, false);
        df.printSchema();
    }
}
