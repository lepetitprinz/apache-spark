package ingestion.stream;

import java.util.concurrent.TimeoutException;

import ingestion.stream.utils.lib.StreamingUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReadLinesFromFileStreamApp {
    private static Logger log = LoggerFactory
            .getLogger(ReadLinesFromFileStreamApp.class);

    public static void main(String[] args) {
        ReadLinesFromFileStreamApp app = new ReadLinesFromFileStreamApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start)-");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines from a file stream")
                .master("local")
                .getOrCreate();
        log.debug("Spark session initiated");

        Dataset<Row> df = spark
                .readStream()
                .format("text")
                .load(StreamingUtils.getInputDirectory());
        log.debug("Dataframe read from stream");

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .option("trucate", false)
                .option("numRows", 3)
                .start();
        log.debug("Query ready");

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error(
                    "Exception while waiting for query to end {}.",
                    e.getMessage(),
                    e
            );
            log.debug("<- start()");
        }
    }
}
