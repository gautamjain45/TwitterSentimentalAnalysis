import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.SparkContext._
import org.apache.spark.streaming.twitter._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark._
import org.apache.spark.rdd._
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._
import org.apache.spark.sql
import org.apache.spark.storage.StorageLevel
import scala.io.Source
import scala.collection.mutable.HashMap
import java.io.File

object mapr {
  def mian(args:Array[String]): Unit =
  {
    if(args.length < 4)
      {
        System.err.prinln("Error")
        System.exit(1)
      }
    StreamingExamples.setStreamingLogLevels()
    val Array(consumerKey,consumerSecret,accessToken,accessTokenSecret) = args.take(4)
    val filters = args.takeRight(args.length - 4)

    System.setProperty("twitter4j.oauth.consumerKey",consumerKey)
    System.setProperty("twitter4j.oauth.consumerSecret",consumerSecret)
    System.setProperty("twitter4j.oauth.accessToken",accessToken)
    System.setProperty("twitter4j.oauth.accessTokenSecret",accessToken)

    val sparkConf = new SparkConf().setAppName("SentimentalAnalysis").setMaster("local[2]")
    val ssc = new StreamingContext(sparkConf,Seconds(5))
    val stream = TwitterUtils.createStream(ssc,None,filters)

    val tags = stream.flatMap { status => status.getHashtagEntities.map(_.getText)}

    tags.countByValue().foreachRDD { rdd =>
      val now = org.joda.time.DateTime.now()
      rdd.sortBy(_._2).map(x => (x,now)).saveAsTextFile(s"~/twitter/$now")
    }

    val tweets = stream.filter { t =>
      val tags = t.getText.split(" ").filter(_.startsWith("#")).map(_.toLowerCase)
      tags.exists{ x => true}
    }

    val data = tweets.map { status =>
      val sentiment = SentimentalAnalysis.Utils.detectSentiment(Status.getText)
      val tagss = status.getHashtagEntities.map(_.getText.LowerCase)
      (status.getText,sentiment.toString,tagss.toString())
    }

    data.print()
    data.saveAsTextFiles("~/twitterss","20000")

    ssc.start()
    ssc.awaitTermination()
  }
}