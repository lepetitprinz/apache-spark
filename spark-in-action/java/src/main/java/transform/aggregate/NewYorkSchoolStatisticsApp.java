package transform.aggregate;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.sum;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.floor;
import static org.apache.spark.sql.functions.substring;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NewYorkSchoolStatisticsApp {
    private static Logger log = LoggerFactory.getLogger(NewYorkSchoolStatisticsApp.class);

    private SparkSession spark = null;

    public static void main(String[] args) {
        NewYorkSchoolStatisticsApp app = new NewYorkSchoolStatisticsApp();
        app.start();
    }

    private void start() {
        // Creates a session on a local master
        spark = SparkSession.builder()
            .appName("NYC schools analytics")
            .master("local[*]")
            .getOrCreate();

        spark.sparkContext().setLogLevel("ERROR");

        String fileName = "data/ch15/nyc_school_attendance/";
        Dataset<Row> masterDf =
            loadDataUsing2018Format(fileName + "2018*.csv");

        masterDf = masterDf.unionByName(
            loadDataUsing2015Format(fileName + "2015*.csv"));

        masterDf = masterDf.unionByName(
            loadDataUsing2006Format(
                fileName + "200*.csv",
                fileName + "2012*.csv"));

        masterDf = masterDf.cache();

        log.debug("Dataset contains {} rows", masterDf.count());
        masterDf.sample(.5).show(5);
        masterDf.printSchema();

        // Unique schools
        Dataset<Row> uniqueSchoolDf = masterDf.select("schoolId").distinct();
        log.debug("Dataset contains {} unique schools", uniqueSchoolDf.count());

        // Calculating the average enrollment for each school
        Dataset<Row> averageEnrollmentDf = masterDf
            .groupBy(col("schoolId"), col("schoolYear"))
            .avg("enrolled", "present", "absent")
            .orderBy("schoolId", "schoolYear");

        log.info("Average enrollment for each school");
        averageEnrollmentDf.show(20);

        // Evolution of # of students in the schools
        Dataset<Row> studentCountPerYearDf = averageEnrollmentDf
            .withColumnRenamed("avg(enrolled)", "enrolled")
            .groupBy(col("schoolYear"))
            .agg(sum("enrolled").as("enrolled"))
            .withColumn("enrolled", floor("enrolled").cast(DataTypes.LongType))
            .orderBy("schoolYear");

        log.info("Evolution of # of students per year");
        studentCountPerYearDf.show(20);

        // Get the year of the largest enrollment
        Row maxStudentRow = studentCountPerYearDf
            .orderBy(col("enrolled").desc())
            .first();

        String year = maxStudentRow.getString(0);
        long max = maxStudentRow.getLong(1);
        log.debug("{} was the year most students, the district served {} students", year, max);

        // Evolution of # of students in the schools
        Dataset<Row> relativeStudentCountPerYearDf = studentCountPerYearDf
            .withColumn("max", lit(max))
            .withColumn("delta", expr("max - enrolled"))
            .drop("max")
            .orderBy("schoolYear");
        log.info("Variation on the enrollment from {}:", year);
        relativeStudentCountPerYearDf.show(20);

        // Most enrolled per school for each year
        Dataset<Row> maxEnrolledPerSchoolDf = masterDf
            .groupBy(col("schoolId"), col("schoolYear"))
            .max("enrolled")
            .orderBy("schoolId", "schoolYear");
        log.info("Maximum enrollement per school and year");
        maxEnrolledPerSchoolDf.show(20);

        // Min absent per school for each year
        Dataset<Row> minAbsenteeDf = masterDf
            .groupBy(col("schoolId"), col("schoolYear"))
            .min("absent")
            .orderBy("schoolId", "schoolYear");
        log.info("Minimum absenteeism per school and year");
        minAbsenteeDf.show(20);

        // Min absent per school for each year, as a % of enrolled
        Dataset<Row> absenteeRatioDf = masterDf
            .groupBy(col("schoolId"), col("schoolYear"))
            .agg(
                max("enrolled").alias("enrolled"),
                avg("absent").as("absent")
            );

        absenteeRatioDf = absenteeRatioDf
            .groupBy(col("schoolId"))
            .agg(
                avg("enrolled").as("avg_enrolled"),
                avg("absent").as("avg_absent"))
            .withColumn("%", expr("avg_absent / avg_enrolled * 100"))
            .filter(col("avg_enrolled").$greater(10))
            .orderBy("%", "avg_enrolled");
        log.info("Schools with the least absenteeism");
        absenteeRatioDf.show(5);

        log.info("Schools with the most absenteeism");
        absenteeRatioDf
            .orderBy(col("%").desc())
            .show(5);
    }


    private Dataset<Row> loadDataUsing2018Format(String... fileNames) {
        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("schoolId", DataTypes.StringType, false),
            DataTypes.createStructField("date", DataTypes.DateType, false),
            DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
            DataTypes.createStructField("present", DataTypes.IntegerType, false),
            DataTypes.createStructField("absent", DataTypes.IntegerType, false),
            DataTypes.createStructField("released", DataTypes.IntegerType, false)
        });

        Dataset<Row> df = spark.read()
            .format("csv")
            .option("header", "true")
            .option("dateFormat", "yyyyMMdd")
            .schema(schema)
            .load(fileNames);

        df = df.withColumn("schoolYear", lit(2018));

        return df;
    }

    private Dataset<Row> loadDataUsing2006Format(String... fileNames) {
        return loadData(fileNames, "yyyyMMdd");
    }

    private Dataset<Row> loadDataUsing2015Format(String... fileNames) {

        return loadData(fileNames, "MM/dd/yyyy");
    }

    /*
     * Common loader for datasets, accepts a date format
     */
    private Dataset<Row> loadData(String[] fileNames, String dateFormat) {
        StructType schema = DataTypes.createStructType(new StructField[] {
            DataTypes.createStructField("schoolId", DataTypes.StringType, false),
            DataTypes.createStructField("date", DataTypes.DateType, false),
            DataTypes.createStructField("schoolYear", DataTypes.StringType, false),
            DataTypes.createStructField("enrolled", DataTypes.IntegerType, false),
            DataTypes.createStructField("present", DataTypes.IntegerType, false),
            DataTypes.createStructField("absent", DataTypes.IntegerType, false),
            DataTypes.createStructField("released", DataTypes.IntegerType, false)
        });

        Dataset<Row> df = spark.read().format("csv")
            .option("header", "true")
            .option("dateFormat", dateFormat)
            .schema(schema)
            .load(fileNames);

        df = df.withColumn("schoolYear", substring(col("schoolYear"), 1, 4));

        return df;
    }
}
