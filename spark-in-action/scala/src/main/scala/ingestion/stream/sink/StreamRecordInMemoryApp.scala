package ingestion.stream.sink

import ingestion.stream.utils.StreamingUtils
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

class StreamRecordInMemoryApp {
  private val log = LoggerFactory.getLogger(classOf[StreamRecordInMemoryApp])

  def start(): Unit = {
    val spark = SparkSession.builder
      .appName("Read lines over a file stream")
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
      .csv(StreamingUtils.getInputDirectory)

    val query = df.writeStream
      .outputMode(OutputMode.Append)
      .format("memory")
      .option("queryName", "people")
      .start

    var queryInMemoryDf: DataFrame = null
    var iterationCount = 0
    val start = System.currentTimeMillis

    while (query.isActive) {
      queryInMemoryDf = spark.sql("SELECT * FROM people")
      iterationCount += 1
      log.debug(s"Pass #$iterationCount, dataframe contains ${queryInMemoryDf.count} records")

      queryInMemoryDf.show()

      if (start + 60000 < System.currentTimeMillis)
        query.stop()

      try {
        Thread.sleep(2000)
      } catch {
        case e: InterruptedException =>
        // Simply ignored
      }
    }

    log.debug("<- start()")
  }
}

object StreamRecordInMemoryApplication {

  def main(args: Array[String]): Unit = {
    val app = new StreamRecordInMemoryApp
    app.start()
  }

}