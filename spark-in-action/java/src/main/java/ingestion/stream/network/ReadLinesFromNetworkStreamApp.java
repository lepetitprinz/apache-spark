package ingestion.stream.network;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class ReadLinesFromNetworkStreamApp {
    private static Logger log = LoggerFactory.getLogger(
            ReadLinesFromNetworkStreamApp.class
    );

    public static void main(String[] args) {
        ReadLinesFromNetworkStreamApp app = new ReadLinesFromNetworkStreamApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        log.debug("-> start()");

        SparkSession spark = SparkSession.builder()
                .appName("Read lines over a network stream")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark
                .readStream()
                .format("socket")
                .option("host", "localhost")
                .option("port", 9999)
                .load();

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        try {
            query.awaitTermination(60000);
        } catch (StreamingQueryException e) {
            log.error(
                    "Exception while waiting for query to end {}", e.getMessage(), e
            );
        }

        log.debug("Query status: {}", query.status());
        log.debug("<- start()");
    }
}
