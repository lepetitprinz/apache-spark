package performance;

import static org.apache.spark.sql.functions.col;

import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.util.List;

public class CacheCheckpointApp {
    enum Mode { NO_CACHE_NO_CHECKPOINT, CACHE, CHECKPOINT, CHECKPOINT_NON_EAGER }

    private SparkSession spark;

    public static void main(String[] args) {
        CacheCheckpointApp app = new CacheCheckpointApp();
        app.start();
    }

    private void start() {
        this.spark = SparkSession.builder()
            .appName("Example of cache and checkpoint")
            .master("local[*]")
            .config("spark.executor.memory", "16g")  // Specifies that the executor memory
            .config("spark.driver.memory", "8g")  // Specifies that the driver memory
            .config("spark.memory.offHeap.enabled", "true")  // Tells Spark to use the memory off heap
            .config("spark.memory.offHeap.size", "4g")  // Specifies the size of the memory off the heap
            .getOrCreate();

        SparkContext sc = spark.sparkContext();
        sc.setLogLevel("ERROR");

        sc.setCheckpointDir("checkpoint");

        // Specify the number of records to generate
        int recordCount = 1000;

        // Create and process the records without cache or checkpoint
        long t0 = processDataframe(recordCount, Mode.NO_CACHE_NO_CHECKPOINT);

        // Create and process the records with cache
        long t1 = processDataframe(recordCount, Mode.CACHE);

        // Create and process the records with a checkpoint
        long t2 = processDataframe(recordCount, Mode.CHECKPOINT);

        // Create and process the records with a checkpoint
        long t3 = processDataframe(recordCount, Mode.CHECKPOINT_NON_EAGER);

        System.out.println("\nProcessing times");
        System.out.println("Without cache ............... " + t0 + " ms");
        System.out.println("With cache .................. " + t1 + " ms");
        System.out.println("With checkpoint ............. " + t2 + " ms");
        System.out.println("With non-eager checkpoint ... " + t3 + " ms");

    }

    private long processDataframe(int recordCount, Mode mode) {
        Dataset<Row> df =
            RecordGeneratorUtils.createDataframe(this.spark, recordCount);

        long t0 = System.currentTimeMillis();
        Dataset<Row> topDf = df.filter(col("rating").equalTo(5));
        topDf= addOption(topDf, mode);

        List<Row> langDf =
            topDf.groupBy("lang").count().orderBy("lang").collectAsList();
        List<Row> yearDf =
            topDf.groupBy("year").count().orderBy(col("year").desc())
                .collectAsList();

        long t1 = System.currentTimeMillis();

        System.out.println("Processing took " + (t1 - t0) + " ms.");

        System.out.println("5 start publications per language");
        for (Row r : langDf) {
            System.out.println(r.getString(0) + " ... " + r.getLong(1));
        }

        System.out.println("5 start publications per year");
        for (Row r : yearDf) {
            System.out.println(r.getInt(0) + " ... " + r.getLong(1));
        }

        return t1 - t0;
    }

    private Dataset<Row> addOption(Dataset<Row> df, Mode mode) {
        switch (mode) {
            case CACHE:
                df = df.cache();
                break;
            case CHECKPOINT:
                df = df.checkpoint();
                break;
            case CHECKPOINT_NON_EAGER:
                df = df.checkpoint(false);
                break;
        }
        return df;
    }
}
