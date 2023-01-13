package ingestion.stream.sink;

import ingestion.stream.utils.lib.StreamingUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class StreamRecordOutputParquetApp {
    private static Logger log = LoggerFactory.getLogger(
            StreamRecordOutputParquetApp.class
    );

    public static void main(String[] args) {
        StreamRecordOutputParquetApp app = new StreamRecordOutputParquetApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines over a file stream")
                .master("local")
                .getOrCreate();

        // The record structure must match the structure of generated record
        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");

        Dataset<Row> df = spark
                .readStream()
                .format("csv")
                .schema(recordSchema)
                .csv(StreamingUtils.getInputDirectory());

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())  // File output only supports append
                .format("parquet")  // Apache Parquet
                .option("path", "output/parquet")  // output directory
                .option("checkpointLocation", "checkpoint")  // check potin
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end {}", e.getMessage(), e);
        }

        log.debug("<- start()");
    }
}