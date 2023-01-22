package stream

import org.apache.spark.sql.functions.{expr, regexp_replace, split}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.streaming.{DataStreamWriter, OutputMode, StreamingQuery, StreamingQueryException}
import org.apache.spark.sql.types.{StringType, StructType}

object StreamMongodbSinkScalaApp {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("Twitter Streaming data to JSON ")
      .master("local")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    val schema =  new StructType()
      .add("tweetId", StringType)
      .add("seqId", StringType)
      .add("rawData", StringType)

    var df: DataFrame = spark
      .readStream
      .format("csv")
      .schema(schema)
      .option("header", "true")
      .option("delimiter", ",")
      .option("encoding", "utf-8")
      .load("data/stream")

    df = df.drop("tweetId")
    df = df.withColumn("dataSplit", split(df.col("rawData"), " "))

    // Remove retweet id
    df = df.withColumn("data", expr("filter(dataSplit, x -> x not rlike '@')"))

    df = df.withColumn("data", expr("filter(data, x -> x !='')"))

    // Remove special characters
    df = df.withColumn("data", expr("filter(data, x -> x rlike '^[ㄱ-ㅎ|가-힣|a-z|A-Z]+$')"))

    // Remove meaningless words
    df = df.withColumn("data", regexp_replace(df.col("data"), "RT", ""))

    df = df.withColumn("data", regexp_replace(df.col("data"), "[{}\\[\\]]", ""))

    df = df.withColumn("data", regexp_replace(df.col("data"), "^\\s+$", ""))

    df = df.drop("rawData", "dataSplit")

    val dataStreamWriter: DataStreamWriter[Row] = df
      .writeStream
      .format("mongodb")
      .option("spark.mongodb.connection.uri", "mongodb://localhost:27017")
      .option("spark.mongodb.database", "twitter")
      .option("spark.mongodb.collection", "stream")
      .option("checkpointLocation", "data/output/mongodb/checkpoint")
      .outputMode("append")

    val query: StreamingQuery = dataStreamWriter.start()

    try {
      query.awaitTermination(5000)
    } catch {
      case e: StreamingQueryException =>
        println("Exception while waiting for query to end")
    }
  }
}
