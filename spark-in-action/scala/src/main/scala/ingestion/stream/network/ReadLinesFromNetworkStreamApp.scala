package ingestion.stream.network

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.slf4j.LoggerFactory

class ReadLinesFromNetworkStreamApp {
  private val log = LoggerFactory.getLogger(classOf[ReadLinesFromNetworkStreamApp])

  def start(): Unit = {
    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a network stream")
      .master("local[*]")
      .getOrCreate

    val df = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", 9999)
      .load

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}")
    }

    log.debug("Query status: {}", query.status)
    log.debug("<- start()")
  }

}
