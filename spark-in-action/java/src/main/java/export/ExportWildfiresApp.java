package export;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.round;
import static org.apache.spark.sql.functions.isnull;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.unix_timestamp;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Paths;

public class ExportWildfiresApp {
    private static Logger log = LoggerFactory.getLogger(ExportWildfiresApp.class);

    public static void main(String[] args) {
        ExportWildfiresApp app = new ExportWildfiresApp();
        app.start();
    }

    private boolean start() {
        if (!downloadDatafiles()) {
            System.out.println("Downloading is failed");
            return false;
        }

        SparkSession spark = SparkSession.builder()
            .appName("Wildfire data pipeline")
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        // Format the VIIRS dataset
        Dataset<Row> viirsDf = spark.read().format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(Paths.get(K.TMP_STORAGE, K.VIIRS_FILE).toString());

        viirsDf = viirsDf
            .withColumn("acq_time_min", expr("acq_time % 100"))
            .withColumn("acq_time_hr", expr("int(acq_time / 100"))
            .withColumn(
                "acq_time2",
                unix_timestamp(col("acq_date").cast(DataTypes.DateType)))
            .withColumn("acq_time3", expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
            .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
            .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
            .withColumnRenamed("confidence", "confidence_level")
            .withColumn("brightness", lit(null))
            .withColumn("bright_t31", lit(null));

        viirsDf.show();
        viirsDf.printSchema();

        Dataset<Row> df = viirsDf.groupBy("confidence_level").count();
        long count = viirsDf.count();
        df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
        df.show();

        // Format the MODIF dataset
        int low = 40;
        int high = 100;
        Dataset<Row> modisDf = spark.read().format("csv")
            .option("header", true)
            .option("inferSchema", true)
            .load(Paths.get(K.TMP_STORAGE, K.MODIS_FILE).toString());

        modisDf = modisDf
            .withColumn("acq_time_min", expr("acq_time % 100"))
            .withColumn("acq_time_hr", expr("int(acq_time / 100)"))
            // change from the book: to have a proper type in the schema
            .withColumn("acq_time2", unix_timestamp(col("acq_date").cast(DataTypes.DateType)))
            .withColumn(
                "acq_time3",
                expr("acq_time2 + acq_time_min * 60 + acq_time_hr * 3600"))
            .withColumn("acq_datetime", from_unixtime(col("acq_time3")))
            .drop("acq_date", "acq_time", "acq_time_min", "acq_time_hr", "acq_time2", "acq_time3")
            .withColumn(
                "confidence_level",
                when(col("confidence").$less$eq(low), "low"))
            .withColumn(
                "confidence_level",
                when(col("confidence").$greater(low)
                    .and(col("confidence").$less(high)), "normal"))
            .withColumn("confidence_level",
                when(isnull(col("confidence_level")), "high")
                    .otherwise(col("confidence_level")))
            .drop("confidence")
            .withColumn("bright_ti4", lit(null))
            .withColumn("bright_ti5", lit(null));

        modisDf.show();
        modisDf.printSchema();

        df = modisDf.groupBy("confidence_level").count();
        count = modisDf.count();
        df = df.withColumn("%", round(expr("100 / " + count + " * count"), 2));
        df.show();

        Dataset<Row> wildfireDf = viirsDf.unionByName(modisDf);
        wildfireDf.show();
        wildfireDf.printSchema();

        log.info("# of partitions: {}", wildfireDf.rdd().getNumPartitions());

        wildfireDf
            .write()
            .format("parquet")
            .mode(SaveMode.Overwrite)
            .save("data/output/ch17/fires_parquet");

        Dataset<Row> outputDf = wildfireDf
            .filter("confidence_level = 'high'")
            .repartition(1);  // Forces Spark to move the data to one partition
        outputDf
            .write()
            .format("csv")  // Specify export format
            .option("header", true)
            .mode(SaveMode.Overwrite)  // Specify save mode
            .save("data/output/ch17/high_confidence_fires_csv");

        return true;
    }


    private boolean downloadDatafiles() {
        log.trace("-> downloadDatafiles()");
        // Download the data file
        String fromFile =
            "https://firms.modaps.eosdis.nasa.gov/data/active_fire/c6/csv/"
                + K.MODIS_FILE;
        String toFile = Paths.get(K.TMP_STORAGE, K.MODIS_FILE).toString();

        if (!download(fromFile, toFile)) {
            return false;
        }

        return true;
    }

    private boolean download(String fromFile, String toFile) {
        try {
            URL website = new URL(fromFile);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream(toFile);
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();
            rbc.close();
        } catch (IOException e) {
            log.debug("ERROR while downloading '{}', got: {}", fromFile, e.getMessage(), e);
            return false;
        }

        log.debug("{} downloaded successfully.", toFile);
        return true;
    }
}
