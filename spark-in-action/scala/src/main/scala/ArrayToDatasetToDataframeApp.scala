import java.util.{Arrays, List}
import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
object ArrayToDatasetToDataframeApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Array to dataframe")
      .master("local")
      .getOrCreate()

    val stringList = Array[String]("Jean", "Liz", "Pierre", "Lauric")
    val data: List[String] = Arrays.asList(stringList:_*)
    val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)
    ds.show()
    ds.printSchema()

    val df = ds.toDF
    df.show()
    df.printSchema()
  }
}
