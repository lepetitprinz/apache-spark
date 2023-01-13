package ingestion.stream.utils

object StreamingUtils {

  def getInputDirectory: String =
    if (System.getProperty("os.name").toLowerCase.startsWith("win")) "C:\\TEMP\\"
    else "data"
}
