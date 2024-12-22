package App.ApiController

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import scala.io.Source
import play.api.libs.json._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import akka.actor.ActorSystem

object TwitterKafkaProducerApp {
  implicit val system: ActorSystem = ActorSystem("TwitterStreamSimulator")

  // Define a case class to parse the tweet JSON
  case class Tweet(created_at: String, id: Long, text: String, user: User)
  case class User(name: String, screen_name: String, location: Option[String])

  // Implicit JSON Reads
  implicit val userReads: Reads[User] = Json.reads[User]
  implicit val tweetReads: Reads[Tweet] = Json.reads[Tweet]

  def main(args: Array[String]): Unit = {
    val kafkaTopic = "tweet-stream"
    val kafkaBroker = "localhost:9092"

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBroker)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // Fix: Create a KafkaProducer instance correctly
    val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

    val filePath = "boulder_flood_geolocated_tweets.json"
    val tweetSource = Source.fromFile(filePath)

    try {
      // Start a simulation of a continuous stream
      val lines = tweetSource.getLines().toList
      val delay = 1.second // Set a delay between each "tweet"

      // Function to process each tweet
      def processTweet(line: String): Unit = {
        val json = Json.parse(line)
        json.validate[Tweet] match {
          case JsSuccess(tweet, _) =>
            println(s"Tweet from ${tweet.user.screen_name}: ${tweet.text}")

            // Produce the tweet to Kafka
            val record = new ProducerRecord[String, String](kafkaTopic, tweet.id.toString, Json.stringify(json))
            kafkaProducer.send(record)
          case JsError(errors) =>
            println(s"Failed to parse tweet: $errors")
        }
      }

      // Stream simulation with Akka
      lines.zipWithIndex.foreach { case (line, index) =>
        system.scheduler.scheduleOnce(index * delay) {
          processTweet(line)
        }
      }

      // Keep the system running for the duration of the streaming
      system.scheduler.scheduleOnce((lines.length * delay) + 2.seconds) {
        println("Streaming complete. Shutting down...")
        kafkaProducer.close() // Close KafkaProducer
        system.terminate()
      }
    } finally {
      tweetSource.close()
    }
  }
}
