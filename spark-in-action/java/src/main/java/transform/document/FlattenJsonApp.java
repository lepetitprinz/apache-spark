package transform.document;

import static org.apache.spark.sql.functions.explode;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

public class FlattenJsonApp {
    public static final String ARRAY_TYPE = "Array";
    public static final String STRUCT_TYPE = "Struc";

    public static void main(String[] args) {
        FlattenJsonApp app = new FlattenJsonApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Flatten JSON data")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Reads a JSON, stores it in a dataframe
        Dataset<Row> invoicesDf = spark.read()
                .format("json")
                .option("multiline", "true")
                .load("data/ch13/json/nested_array.json");

        invoicesDf.show(3);
        invoicesDf.printSchema();

        Dataset<Row> flatInvoicesDf = flattenNestedStructure(spark, invoicesDf);
        flatInvoicesDf.show(20, false);
        flatInvoicesDf.printSchema();
    }

    public static Dataset<Row> flattenNestedStructure(
            SparkSession spark,
            Dataset<Row> df) {
        boolean recursion = false;

        Dataset<Row> processedDf = df;
        StructType schema = df.schema();
        StructField fields[] = schema.fields();
        for (StructField field : fields) {
            switch (field.dataType().toString().substring(0, 5)) {
                case ARRAY_TYPE:
                    processedDf = processedDf
                            .withColumnRenamed(field.name(), field.name() + "_tmp");
                    processedDf = processedDf.withColumn(
                            field.name(),
                            explode(processedDf.col(field.name() + "_tmp")));
                    processedDf = processedDf.drop(field.name() + "_tmp");
                    recursion = true;
                    break;
                case STRUCT_TYPE:
                    // Mapping
                    StructField[] structFields = ((StructType) field.dataType()).fields();
                    for (StructField structField : structFields) {
                        processedDf = processedDf.withColumn(
                                field.name() + "_" + structField.name(),
                                processedDf.col(field.name() + "." + structField.name()));
                    }
                    processedDf = processedDf.drop(field.name());
                    recursion = true;
                    break;
            }
        }
        if (recursion) {
            processedDf = flattenNestedStructure(spark, processedDf);
        }
        return processedDf;
    }
}
