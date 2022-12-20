package laziness;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import static org.apache.spark.sql.functions.expr;

public class TransformationAndActionApp {

    public static void main(String[] args) {
        TransformationAndActionApp app = new TransformationAndActionApp();
        String mode = "noop";
        if (args.length != 0) {
            mode = args[0];
        }
        app.start(mode);
    }

    private void start(String mode) {
        long t0 = System.currentTimeMillis();  // set the timer

        // Step 1 - creates a session on a local master
        SparkSession spark = SparkSession.builder()
                .appName("Analysing Catalyst's behavior")
                .master("local")
                .getOrCreate();

        long t1 = System.currentTimeMillis(); // Measures the time spent creating the session
        System.out.println("1. Creating a session ..... " + (t1 - t0));

        // Step 2 - reads a csv file with header
        Dataset<Row> df = spark.read().format("csv")
                .option("header", "true")
                .load("data/NCHS_-_Teen_Birth_Rates_for_Age_Group_15-19_in_the_United_States_by_County.csv");

        Dataset<Row> initlaDF = df;
        long t2 = System.currentTimeMillis();
        System.out.println("2. Loading initial dataset ..... " + (t2 - t1));

        // Step3 - build a bigger dataset
        for (int i = 0; i < 10; i++) {
            df = df.union(initlaDF);
        }
        long t3 = System.currentTimeMillis();
        System.out.println("3. Building full dataset ..... " + (t3 - t2));

        // Step 4 - cleanup
        df = df.withColumnRenamed("Lower Confidence Limit", "lcl");
        df = df.withColumnRenamed("Upper Confidence Limit", "ucl");

        long t4 = System.currentTimeMillis();
        System.out.println("4. Clean-up ..... " + (t4 - t3));

        // Step 5 - Transformation
        if (mode.compareToIgnoreCase("noop") != 0) {
            df = df
                    .withColumn("avg", expr("(lcl+ucl/2"))
                    .withColumn("lcl2", df.col("lcl"))
                    .withColumn("ulc2", df.col("ucl"));
            if (mode.compareToIgnoreCase("full") == 0) {
                df = df
                        .drop(df.col("avg"))
                        .drop(df.col("lcl2"))
                        .drop(df.col("ucl2"));
            }
        }

        long t5 = System.currentTimeMillis();
        System.out.println("5. Transformations ..... " + (t5 - t4));

        // Step 6 - Action
        df.collect();  // Performs the collection action
        long t6 = System.currentTimeMillis();
        System.out.println("6. Final action ..... " + (t6 - t5));

        System.out.println("");
        System.out.println("# of records ..... " + df.count());
    }
}
