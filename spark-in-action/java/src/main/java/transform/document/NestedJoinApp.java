package transform.document;

import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.collect_list;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

public class NestedJoinApp {
    private static Logger log = LoggerFactory.getLogger(NestedJoinApp.class);

    public static final String TEMP_COL = "temp_column";

    public static void main(String[] args) {
        NestedJoinApp app = new NestedJoinApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("Using Nested Join for data transformation")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> businessDf = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/ch13/orangecounty_restaurants/businesses.csv");

        Dataset<Row> inspectionDf = spark.read()
                .format("csv")
                .option("header", "true")
                .load("data/ch13/orangecounty_restaurants/inspections.csv");

        businessDf.show(3);
        businessDf.printSchema();

        inspectionDf.show(3);
        inspectionDf.printSchema();

        Dataset<Row> factSheetDf = nestedJoin(
                businessDf,
                inspectionDf,
                "business_id",
                "business_id",
                "inner",
                "inspections");

        factSheetDf.show(3);
        factSheetDf.printSchema();
    }

    private Dataset<Row> nestedJoin(
            Dataset<Row> leftDf,
            Dataset<Row> rightDf,
            String leftJoinCol,
            String rightJoinCol,
            String joinType,
            String nestedCol) {

        // Perform the inner join
        Dataset<Row> resDf = leftDf.join(
                rightDf,
                rightDf.col(rightJoinCol).equalTo(leftDf.col(leftJoinCol)),
                joinType);

        // Makes a list of the left columns
        Column[] leftColumns = getColumns(leftDf);
        if (log.isDebugEnabled()) {
            log.debug("We have {} columns to work with: {}",
                    leftColumns.length,
                    Arrays.toString(leftColumns));
            log.debug("Schema and data: ");
            resDf.printSchema();
            resDf.show(3);
        }

        // Copies all the columns from the left/master
        Column[] allColumns =
                Arrays.copyOf(leftColumns, leftColumns.length + 1);

        // Adds a column, which is a structure containing all the columns form the detail
        allColumns[leftColumns.length] =
                struct(getColumns(rightDf)).alias(TEMP_COL);

        // Performs a select on all columns
        resDf = resDf.select(allColumns);
        if (log.isDebugEnabled()) {
            resDf.printSchema();
            resDf.show(3);
        }

        resDf = resDf
                .groupBy(leftColumns)
                .agg(collect_list(col(TEMP_COL)).as(nestedCol));

        if (log.isDebugEnabled()) {
            resDf.printSchema();
            resDf.show(3);
            log.debug("After nested join, we have {} rows.", resDf.count());
        }

        return resDf;
    }

    private static Column[] getColumns(Dataset<Row> df) {
        String[] fieldnames = df.columns();
        Column[] columns = new Column[fieldnames.length];
        int i = 0;
        for (String fieldname: fieldnames) {
            columns[i++] = df.col(fieldname);
        }

        return columns;
    }
}

