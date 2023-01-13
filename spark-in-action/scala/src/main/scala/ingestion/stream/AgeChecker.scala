package ingestion.stream

import org.apache.spark.sql.{ForeachWriter, Row}
import org.slf4j.LoggerFactory

class AgeChecker extends ForeachWriter[Row] {

  private val log = LoggerFactory.getLogger(classOf[AgeChecker])
  private var streamId = 0

  def this(streamId: Int) = {
    this()
    this.streamId = streamId
  }

  override def close(arg0: Throwable): Unit = {}
  override def open(args0: Long, arg1: Long) = true

  def process(arg0: Row): Unit = {
    if (arg0.length !=5 ) return
    val age = arg0.getInt(3)

    if (age < 13)
      log.debug(s"one stream #${streamId}: ${arg0.getString(0)} is a kid")
    else if (age < 20)
      log.debug(s"one stream #${streamId}: ${arg0.getString(0)} is a teen")
    else if (age > 64)
      log.debug(s"one stream #${streamId}: ${arg0.getString(0)} is a senior")
  }

}
