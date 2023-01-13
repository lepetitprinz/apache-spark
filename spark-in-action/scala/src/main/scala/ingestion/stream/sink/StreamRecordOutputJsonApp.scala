package ingestion.stream.sink

import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

/**
 * Saves the record in the stream in a json file.
 *
 */
class StreamRecordOutputJsonApp {

  private val log = LoggerFactory.getLogger(classOf[StreamRecordOutputJsonApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over a file stream then save in json file")
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
      .format("json")
      .option("path", "data/output/json")
      .option("checkpointLocation", "data/checkpoint")
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
object StreamRecordOutputJsonApplication {

  def main(args: Array[String]): Unit = {

    val app = new StreamRecordOutputJsonApp
    app.start()

  }

}