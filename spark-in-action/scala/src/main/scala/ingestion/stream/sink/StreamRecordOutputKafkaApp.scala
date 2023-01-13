package ingestion.stream.sink

import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class StreamRecordOutputKafkaApp {
  private val log = LoggerFactory.getLogger(classOf[StreamRecordOutputKafkaApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a file stream then save in parquet file")
      .master("local[*]")
      .getOrCreate

    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    val df = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .csv(StreamingUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("kafka")
      .option("kafka.bootstrap.servers", "ost1:port1,host2:port2")
      .option("topic", "updates")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.", e)
    }

    log.debug("<- start()")
  }
}

object StreamRecordOutputKafkaApplication {

  def main(args: Array[String]): Unit = {
    val app = new StreamRecordOutputKafkaApp
    app.start()
  }

}
