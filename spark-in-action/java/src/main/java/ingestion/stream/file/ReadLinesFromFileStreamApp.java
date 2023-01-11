package ingestion.stream.file;

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
    // Initialization of the logger
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

        spark.sparkContext().setLogLevel("ERROR");

        Dataset<Row> df = spark
                .readStream()  // read from stream
                .format("text")  // file format is text
                .load(StreamingUtils.getInputDirectory());  // the directory to read from

        log.debug("Dataframe read from stream");

        StreamingQuery query = df
                .writeStream()  // ready to write in a stream
                .outputMode(OutputMode.Append())  // as an append to the output
                .format("console")  // output is the console
                .option("truncate", false)  // records are not truncated
                .option("numRows", 3)       // at most 3 will be displayed
                .start();

        log.debug("Query ready");

        try {
            // if this parameter does not set, the method will wait forever
            query.awaitTermination(60000);  // waits for data to come
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
