package App.ApiController

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import play.api.libs.json._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor.ActorSystem
import play.api.libs.functional.syntax._
import config.AppConfig.Extract_Space.Space
import config.AppConfig.Extract_hashtags._
import config.AppConfig.Sentiment_Analyzing._
import org.apache.spark.ml.PipelineModel
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row}
import org.apache.log4j.{Level, Logger}

object TwitterKafkaProducerApp {
  implicit val system: ActorSystem = ActorSystem("TwitterStreamSimulator")

  // Define case classes to parse the tweet JSON
  case class User(name: String, screen_name: String, location: Option[String])
  case class Tweet(created_at: String, id: Long, text: String, user: User, hashtags: Option[Seq[String]], space: Option[Space])

  implicit val userReads: Reads[User] = Json.reads[User]
  implicit val userWrites: Writes[User] = Json.writes[User]
  implicit val tweetReads: Reads[Tweet] = (
    (__ \ "created_at").read[String] and
      (__ \ "id").read[Long] and
      (__ \ "text").read[String] and
      (__ \ "user").read[User] and
      (__ \ "hashtags").readNullable[Seq[String]] and
      (__ \ "space").readNullable[Space]
    )(Tweet.apply _)
  implicit val tweetWrites: Writes[Tweet] = Json.writes[Tweet]

  def main(args: Array[String]): Unit = {
    val kafkaTopic = "tweet-stream"
    val kafkaBroker = "localhost:9092"
    val modelPath = "C:/BigDataProj/data/trained_model"  // استخدم نفس المسار الذي حفظت فيه النموذج

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBroker)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

    val filePath = "boulder_flood_geolocated_tweets.json"

    // إعداد Spark وتحميل نموذج تحليل المشاعر
    val spark = SparkSession.builder
      .appName("Sentiment Analysis")
      .master("local[*]") // تأكد من تضمين master URL
      .config("spark.ui.showConsoleProgress", "false")
      .getOrCreate()

    // ضبط مستوى التسجيل في Spark إلى ERROR لتقليل عدد رسائل الـ DEBUG
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)

    // استيراد ضمني لتحويل Seq إلى DataFrame
    import spark.implicits._

    // تحميل النموذج المدرب
    val sentimentModel = loadSentimentModel(modelPath)

    def analyzeSentiment(text: String): String = {
      val testData = Seq((1, text)).toDF("id", "text")
      val result = sentimentModel.transform(testData)

      val sentimentPrediction = result.select("prediction").first().getDouble(0) match {
        case 1.0 => "positive"
        case 0.0 => "negative"
        case _ => "neutral"
      }

      sentimentPrediction
    }

    def processTweet(line: String): Unit = {
      val json = Json.parse(line)
      json.validate[Tweet] match {
        case JsSuccess(tweet, _) =>
          val hashtags = extractHashtags(tweet.text)
          val tweetWithHashtags = tweet.copy(hashtags = Some(hashtags))
          val sentiment = analyzeSentiment(tweet.text)
          val result = Json.obj(
            "tweet_id" -> tweet.id,
            "user" -> tweet.user.screen_name,
            "text" -> tweet.text,
            "hashtags" -> hashtags,
            "sentiment" -> sentiment
          )

          println(result)

          // إرسال التغريدة إلى Kafka مع مشاعرها
          val updatedJson = Json.toJson(tweetWithHashtags.copy(text = s"${tweetWithHashtags.text} (Sentiment: $sentiment)"))
          val record = new ProducerRecord[String, String](kafkaTopic, tweet.id.toString, updatedJson.toString())
          kafkaProducer.send(record)
        case JsError(errors) =>
          println(s"Failed to parse tweet: $errors")
      }
    }

    // Schedule fetching and processing tweets every 2 seconds
    system.scheduler.scheduleWithFixedDelay(initialDelay = 0.seconds, delay = 2.seconds) { () =>
      val tweetSource = Source.fromFile(filePath)
      try {
        val lines = tweetSource.getLines().toList
        lines.foreach(processTweet)
      } finally {
        tweetSource.close()
      }
    }

    // Keep the system running
    scala.io.StdIn.readLine()
    println("Shutting down...")
    kafkaProducer.close() // Close KafkaProducer
    system.terminate()
    spark.stop()
  }
}
