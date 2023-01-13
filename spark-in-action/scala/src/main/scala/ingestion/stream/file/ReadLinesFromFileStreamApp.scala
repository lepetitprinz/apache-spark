package ingestion.stream.file

import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.slf4j.LoggerFactory

class ReadLinesFromFileStreamApp {
  private val log = LoggerFactory.getLogger(classOf[ReadLinesFromFileStreamApp])

  def start(): Unit = {
    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines from a file stream")
      .master("local[*]")
      .getOrCreate

    val df = spark.readStream
      .format("text")
      .load(StreamingUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .option("truncate", value = false)
      .option("numRows", 3)
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error("Exception while waiting for query to end {}", e.getMessage)
    }

    log.debug("<- start()")
  }
}

object ReadLinesFromFileStreamApplication {

  def main(args: Array[String]): Unit = {
    val app = new ReadLinesFromFileStreamApp
    app.start
  }
}
