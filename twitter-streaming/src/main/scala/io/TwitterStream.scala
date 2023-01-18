package io

import com.typesafe.config.ConfigFactory
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.DStream
import org.apache.spark.streaming.twitter.TwitterUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import scopt.OptionParser
import org.slf4j.LoggerFactory

object TwitterStream {

  private val log = LoggerFactory.getLogger(this.getClass)
  case class CLIParams(filters: Array[String] = Array.empty)
  def main(args: Array[String]): Unit = {
    val parser = parseArgs("Twitter")

    parser.parse(args, CLIParams()) match {
      case Some(params) =>

        val config = ConfigFactory.load()
        val consumerKey = config.getString("twitter4j.oauth.consumerKey")
        val consumerSecret = config.getString("twitter4j.oauth.consumerSecret")
        val accessToken = config.getString("twitter4j.oauth.accessToken")
        val accessTokenSecret = config.getString("twitter4j.oauth.accessTokenSecret")

        // Set the system properties so that Twitter4j library used by twitter stream
        // can use them to generate OAuth credentials
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey)
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret)
        System.setProperty("twitter4j.oauth.accessToken", accessToken)
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret)

        val spark = new SparkConf()
          .setAppName("Twitter Streaming")
          .setMaster("local")

        val ssc = new StreamingContext(spark, Seconds(10))
        val x = ssc.sparkContext.parallelize(Array(1, 2, 3))
        x.foreach(println(_))

        val filter = Array("twitter")
        val stream = TwitterUtils.createStream(ssc, None, filter)
//        val stream = TwitterUtils.createStream(ssc, None, params.filters)
//        val hashTags = stream.flatMap(tweet => tweet.getText.split(" ").filter(_.startsWith("#")))
//        val topHashTags: DStream[(String, Int)] = ???
//
//        topHashTags.foreachRDD((rdd, ts) => {
//          if (!rdd.partitions.isEmpty) {
//            rdd.collect().foreach {
//              case (count, tag) => log.info("%s (%s tweets)".format(tag, count))
//            }
//          }
//        })

//        topHashTags.saveAsTextFiles("data/stream/")

        ssc.start()
        ssc.awaitTermination()
      case None =>
    }
  }

  def parseArgs(appName: String): OptionParser[CLIParams] = {
    new OptionParser[CLIParams](appName) {
      head(appName, "1.0")
//      opt[String]("filters") required() action { (data, conf) =>
//        conf.copy(filters = conf.filters :+ data)
//      } text "Filters to use twitter api. Example : test,twitter"
    }
  }
}
