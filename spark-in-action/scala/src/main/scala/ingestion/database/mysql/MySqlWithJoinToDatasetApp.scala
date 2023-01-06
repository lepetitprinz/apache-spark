package ingestion.database.mysql

import org.apache.spark.sql.SparkSession

import java.util.Properties

object MySqlWithJoinToDatasetApp {

  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder
      .appName("MySQL with join to Dataframe using JDBC")
      .master("local[*]")
      .getOrCreate

    spark.sparkContext.setLogLevel("ERROR")

    // Using properties
    val props = new Properties
    props.put("user", "root")
    props.put("password", "mysql")
    props.put("useSSL", "false")
    props.put("allowPublicKeyRetrieval", "true")

    val sqlQuery = "select actor.first_name, actor.last_name, film.title, " +
      "film.description " +
      "from actor, film_actor, film " +
      "where actor.actor_id = film_actor.actor_id " +
      "and film_actor.film_id = film.film_id"

    val mySqlUrl = "jdbc:mysql://localhost:3306/sakila"
    val df = spark.read
      .jdbc(mySqlUrl, "(" + sqlQuery + ") actor_film_alias", props)

    df.show(5)
    df.printSchema
    println(s"The dataframe contains ${df.count} records")

    spark.stop
  }

}
