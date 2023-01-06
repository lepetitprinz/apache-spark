package ingestion.database.mysql

import org.apache.spark.sql.SparkSession
import java.util.Properties

object MySqlToDatasetWithPartitionApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL to Dataframe using JDBC with partioning")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "mysql")
    props.put("useSSL", "false")
    props.put("allowPublicKeyRetrieval", "true")

    // Used for partitioning
    props.put("partitionColumn", "film_id")
    props.put("lowerBound", "1")
    props.put("upperBound", "1000")
    props.put("numPartitions", "10")

    val mySqlUrl = "jdbc:mysql://localhost:3306/sakila"
    val df = spark.read.jdbc(mySqlUrl, "film", props)

    // Displays the dataframe and some of its metadata
    df.show(5)
    df.printSchema()
    println(s"The dataframe contains ${df.count} record(s).")
    println(s"The dataframe is split over ${df.rdd.getNumPartitions} partition(s).")

    spark.stop
  }
}
