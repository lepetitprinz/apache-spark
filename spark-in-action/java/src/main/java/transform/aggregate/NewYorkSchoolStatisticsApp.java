package transform.aggregate;

import static org.apache.spark.sql.functions.lit;

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

        Dataset<Row> masterDf = null;
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
}
