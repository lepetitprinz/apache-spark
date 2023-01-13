package ingestion.stream.file

import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.{OutputMode, StreamingQueryException}
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class ReadRecordFromFileStreamApp {

  private val log = LoggerFactory.getLogger(classOf[ReadRecordFromFileStreamApp])

  def start(): Unit = {

    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read records from a file stream")
      .master("local")
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
      .load(StreamingUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Update)
      .format("console")
      .start

    try {
      query.awaitTermination(60000)
    } catch {
      case e: StreamingQueryException =>
        log.error(s"Exception while waiting for query to end ${e.getMessage}.")
    }

    log.debug("<- start()")
  }
}

object ReadRecordFromFileStreamApplication {
  def main(args: Array[String]): Unit = {
    val app = new ReadRecordFromFileStreamApp
    app.start()
  }
}
