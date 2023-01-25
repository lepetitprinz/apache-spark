package transform.document;

import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.lit;

import java.io.Serializable;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


public class CsvWithEmbdeddedJsonApp {
    private static final long serialVersionUID = 19711L;

    private final class Jsonifier implements MapFunction<Row, String> {
        private static final long serialVersionUID = 19712L;

        @Override
        public String call(Row r) throws Exception {
            System.out.println(r.mkString());
            StringBuffer sb = new StringBuffer();
            sb.append("{ \"dept\": \"");
            sb.append(r.getString(0));
            sb.append("\",");

            String s = r.getString(1);
            if (s != null) {
                s = s.trim();
                if (s.charAt(0) == '{') {
                    s = s.substring(1, s.length() - 1);
                }
            }
            sb.append(s);
            sb.append(", \"location\": \"");
            sb.append(r.getString(2));
            sb.append("\"}");

            return sb.toString();
        }
    }

    public static void main(String[] args) {
        CsvWithEmbdeddedJsonApp app = new CsvWithEmbdeddedJsonApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("processing with csv")
                .master("local")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", "true")
                .option("delimiter", "|")
                .csv("data/ch13/misc/csv_with_embedded_json.csv");
        df.show(5, false);
        df.printSchema();

        Dataset<String> ds = df.map(
                new Jsonifier(),
                Encoders.STRING());
        ds.show(5, false);
        ds.printSchema();

        Dataset<Row> dfJson = spark.read().json(ds);
        dfJson.show(5, false);
        dfJson.printSchema();

        dfJson = dfJson
                .withColumn("emp", explode(dfJson.col("employee")));
    }
}
