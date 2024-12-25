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

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBroker)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

    val filePath = "boulder_flood_geolocated_tweets.json"



    def processTweet(line: String): Unit = {
      val json = Json.parse(line)
      json.validate[Tweet] match {
        case JsSuccess(tweet, _) =>
          val hashtags = extractHashtags(tweet.text)

          val tweetWithHashtags = tweet.copy(hashtags = Some(hashtags))
          val updatedJson = Json.toJson(tweetWithHashtags)

          println(s"Tweet from ${tweet.user.screen_name}: ${tweet.text} with hashtags: ${hashtags.mkString(", ")}")

          val record = new ProducerRecord[String, String](kafkaTopic, tweet.id.toString, updatedJson.toString())
          kafkaProducer.send(record)
        case JsError(errors) =>
          println(s"Failed to parse tweet: $errors")
      }
    }

    // Schedule fetching and processing tweets every 2 seconds
    system.scheduler.scheduleAtFixedRate(initialDelay = 0.seconds, interval = 2.seconds) { () =>
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
  }
}
