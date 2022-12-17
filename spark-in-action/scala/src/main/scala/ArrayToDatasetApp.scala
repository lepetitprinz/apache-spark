import java.util.{Arrays, List}
import org.apache.spark.sql.{SparkSession, Dataset, Encoders}

object ArrayToDatasetApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("Array to Dataset<String>")
      .master("local")
      .getOrCreate()

    val stringList: Array[String] = Array[String]("Jean", "Liz", "Pierre", "Lauric")
    val data: List[String] = Arrays.asList(stringList:_*)
    val ds: Dataset[String] = spark.createDataset(data)(Encoders.STRING)

    ds.show()
    ds.printSchema()
  }

}
