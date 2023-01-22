package stream;

import static org.apache.spark.sql.functions.split;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.regexp_replace;

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

public class StreamConsoleApp {
    private static Logger log =
            LoggerFactory.getLogger(StreamConsoleApp.class);
    public static void main(String[] args) {
        StreamConsoleApp app = new StreamConsoleApp();
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
                        "tweetId", DataTypes.StringType, true),
                DataTypes.createStructField(
                        "seqId", DataTypes.StringType, true),
                DataTypes.createStructField(
                        "rawData", DataTypes.StringType, true)
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
        df = df.drop("tweetId");
        df = df.withColumn("dataSplit", split(df.col("rawData"), " "));

        // Remove retweet id
        df = df.withColumn("data", expr("filter(dataSplit, x -> x not rlike '@')"));

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

        df = df.drop("rawData", "dataSplit");

        df.printSchema();

        StreamingQuery query = df
                .writeStream()
                .outputMode(OutputMode.Append())
                .format("console")
                .start();

        try {
            query.awaitTermination(5000);
        } catch (StreamingQueryException e) {
            log.error("Exception while waiting for query to end ", e.getMessage(), e);
        }
    }
}
