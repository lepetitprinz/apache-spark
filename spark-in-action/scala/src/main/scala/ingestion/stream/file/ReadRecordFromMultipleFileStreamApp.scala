package ingestion.stream.file

import ingestion.stream.AgeChecker
import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory
class ReadRecordFromMultipleFileStreamApp {

  private val log = LoggerFactory.getLogger(classOf[ReadRecordFromMultipleFileStreamApp])

  def start(): Unit = {
    log.debug("-> start()")

    val spark = SparkSession.builder
      .appName("Read lines over multiple file stream")
      .master("local[*]")
      .getOrCreate

    val recordSchema = new StructType()
      .add("fname", "string")
      .add("mname", "string")
      .add("lname", "string")
      .add("age", "integer")
      .add("ssn", "string")

    val landingDirectoryStream1 = StreamingUtils.getInputDirectory
    val landingDirectoryStream2 = "data/stream/dir2"

    val dfStream1 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream1)

    val dfStream2 = spark.readStream
      .format("csv")
      .schema(recordSchema)
      .load(landingDirectoryStream2)

    val queryStream1 = dfStream1.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeChecker(1))
      .start

    val queryStream2 = dfStream2.writeStream
      .outputMode(OutputMode.Append)
      .foreach(new AgeChecker(2))
      .start

    val startProcessing = System.currentTimeMillis
    var iterationCount = 0

    while (queryStream1.isActive && queryStream2.isActive) {
      iterationCount += 1
      log.debug("pass # {}", iterationCount)
      if (startProcessing + 60000 < System.currentTimeMillis) {
        queryStream1.stop()
        queryStream2.stop()
      }
      try {
        Thread.sleep(2000)
      } catch {
        case e: InterruptedException =>
      }
    }
    log.debug("<- start()")
  }
}

object ReadRecordFromMultipleFileStreamApplication {

  def main(args: Array[String]): Unit = {

    val app = new ReadRecordFromMultipleFileStreamApp
    app.start()
  }

}