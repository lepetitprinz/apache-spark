package ingestion.stream.sink;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class StreamRecordOutputKafkaApp {
    private static Logger log = LoggerFactory.getLogger(
            StreamRecordOutputKafkaApp.class
    );

    public static void main(String[] args) {
        StreamRecordOutputKafkaApp app = new StreamRecordOutputKafkaApp();
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

        StructType recordSchema = new StructType()
                .add("fname", "string")
                .add("mname", "string")
                .add("lname", "string")
                .add("age", "integer")
                .add("ssn", "string");


    }
}
