package dataframe;

import java.io.Serializable;
import java.text.SimpleDateFormat;

import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.concat;
import static org.apache.spark.sql.functions.expr;
import static org.apache.spark.sql.functions.to_date;

import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;


/**
 * Read a csv file, ingest it in a dataframe, convert the dataframe to a dataset
 * , and vice versa.
 */

// When dealing with maps, a lot of objects may need to become Serializable.
// Spark will tell you at runtime, when this is required.
public class CsvToDatasetToDataframeApp implements Serializable {
    private static final long serialVersionUID = -1L;

    // mapper class that will convert a Row to an instance of Book
    class BookMapper implements MapFunction<Row, Book> {
        private static final long serialVersionUID = -2L;

        public Book call(Row value) throws Exception {
            Book b = new Book();
            b.setId((Integer) value.getAs("id"));
            b.setAuthorId((Integer) value.getAs("authorId"));
            b.setLink((String) value.getAs("link"));
            b.setTitle((String) value.getAs("title"));

            //date case
            String dateAsString = (String) value.getAs("releaseDate");
            if (dateAsString != null) {
                SimpleDateFormat parser = new SimpleDateFormat("M/d/yy");
                b.setReleaseDate(parser.parse(dateAsString));
            }
            return b;
        }
    }
    public static void main(String[] args) {
        CsvToDatasetToDataframeApp app = new CsvToDatasetToDataframeApp();
        app.start();
    }

    private void start() {
        SparkSession spark = SparkSession.builder()
                .appName("csv to dataframe to Dataset<Book> and back")
                .master("local")
                .getOrCreate();

        spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY");

        String PATH = "/Users/yjkim-studio/data/spark/";
        String filename = PATH + "books.csv";
        Dataset<Row> df = spark.read().format("csv")
                .option("inferSchema", "true")
                .option("header", "true")
                .load(filename);

        System.out.println("*** Books ingested in a dataframe");
        df.show(5);
        df.printSchema();

        // Converts the dataframe into a dataset by using a map() function
        Dataset<Book> bookDs = df.map(new BookMapper(), Encoders.bean(Book.class));
        System.out.println("*** Books ar now in a dataset of books");
        bookDs.show(5, 17);
        bookDs.printSchema();

        Dataset<Row> df2 = bookDs.toDF();
        df2 = df2.withColumn(
                "releaseDateAsString",
                concat(
                        expr("releaseDate.year + 1900"), lit("-"), // Java start in 1900
                        expr("releaseDate.month + 1"), lit("-"), // months start at 0
                        df2.col("releaseDate.date")));

        df2 = df2
                .withColumn(
                        "releaseDateAsDate",
                        to_date(df2.col("releaseDateAsString"), "yyyy-MM-dd"))
                        // to_date() transforms a text representation of a date to a real date
                .drop("releaseDateAsString");
        System.out.println("*** Books are back in a dataframe");
        df2.show(5, 13);
        df2.printSchema();
    }
}
