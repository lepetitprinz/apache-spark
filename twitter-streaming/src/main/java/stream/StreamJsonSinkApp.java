package stream;

import static org.apache.spark.sql.functions.*;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.streaming.StreamingQueryException;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class StreamJsonSinkApp {
    private static final String TWEET_ID = "edit_history_tweet_ids";
    private static final String ID = "id";
    private static final String TEXT = "text";
    private static final int TIME_OUT = 1000 * 60;
    private static Logger log =
            LoggerFactory.getLogger(StreamJsonSinkApp.class);
    public static void main(String[] args) {
        StreamJsonSinkApp app = new StreamJsonSinkApp();
        try {
            app.start();
        } catch (TimeoutException e) {
            log.error("A timeout exception has occured: {}", e.getMessage());
        }
    }

    private void start() throws TimeoutException {
        SparkSession spark = SparkSession.builder()
                .appName("Twitter Data Streaming Application")
                .master("local")
                .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Specify the record types
        StructType schema = DataTypes.createStructType(new StructField[]{
                DataTypes.createStructField(
                        TWEET_ID, DataTypes.StringType, true),
                DataTypes.createStructField(
                        ID, DataTypes.StringType, true),
                DataTypes.createStructField(
                        TEXT, DataTypes.StringType, true)
        });

        Dataset<Row> df = spark
                .readStream()
                .format("csv")
                .schema(schema)
                .option("header", "true")
                .option("delimiter", ",")
                .option("encoding", "utf-8")
                .load("data/stream");

        // Data Preprocessing
        // Split twitter data
        df = df.drop(TWEET_ID);
        //df = df.withColumn("dataSplit", split(df.col(TEXT), " "));

        // Remove retweet id
        df = df.withColumn("data", expr("filter(data, x -> x not rlike '@')"));

        df = df.withColumn("data", expr("filter(data, x -> x !='')"));

        // Remove special characters
        df = df.withColumn(
                "data",
                expr("filter(data, x -> x rlike '^[ㄱ-ㅎ|가-힣|a-z|A-Z]+$')"));

        // Remove meaningless words
        df = df.withColumn(
                "data",
                regexp_replace(df.col("data"), "RT", ""));

        df = df.withColumn(
                "data",
                regexp_replace(df.col("data"), "[{}\\[\\]]", ""));

        df = df.withColumn(
                "data",
                regexp_replace(df.col("data"), "^\\s+$", ""));

        df = df.drop(TEXT, "dataSplit");

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("json")
                .option("encoding", "utf-8")
                .option("path", "data/output/")
                .option("checkpointLocation", "data/output/checkpoint")
                .start();

        try {
            query.awaitTermination(TIME_OUT);
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end", e.getMessage(), e);
        }
    }
}
