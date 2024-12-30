package App.ApiController

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties,Response, Indexable,RequestSuccess}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.indexes.{IndexRequest, IndexResponse}
import org.apache.kafka.common.serialization.StringSerializer
import play.api.libs.json._
import akka.actor.ActorSystem
import com.sksamuel.elastic4s.ElasticApi.indexInto
import com.sksamuel.elastic4s.ElasticDsl.IndexHandler

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.io.Source
import scala.concurrent.duration._
import scala.concurrent.Future
import play.api.libs.functional.syntax._

object TwitterKafkaProducerApp {
  implicit val system: ActorSystem = ActorSystem("TwitterStreamSimulator")

  // Define a case class to parse the tweet JSON
  case class User(name: String, screen_name: String, location: Option[String])
  case class Tweet(created_at: String, id: Long, text: String, user: User, hashtags: Option[Seq[String]])

  // Implicit JSON Reads and Writes for User and Tweet
  implicit val userReads: Reads[User] = Json.reads[User]
  implicit val userWrites: Writes[User] = Json.writes[User]

  implicit val tweetReads: Reads[Tweet] = (
    (__ \ "created_at").read[String] and
      (__ \ "id").read[Long] and
      (__ \ "text").read[String] and
      (__ \ "user").read[User] and
      (__ \ "hashtags").readNullable[Seq[String]]
    )(Tweet.apply _)

  implicit val tweetWrites: Writes[Tweet] = Json.writes[Tweet]

  // Custom Indexable for Tweet
  implicit val tweetIndexable: Indexable[Tweet] = new Indexable[Tweet] {
    override def json(t: Tweet): String = {
      Json.stringify(Json.toJson(t))
    }
  }
  // Convert Map[String, Any] to JsObject
  implicit def mapIndexable: Indexable[Map[String, Any]] = new Indexable[Map[String, Any]] {
    override def json(t: Map[String, Any]): String = {
      // Convert the Map to a JsObject and then to a JSON string
      Json.stringify(Json.toJson(t.map {
        case (key, value: String) => key -> JsString(value)
        case (key, value: Long) => key -> JsNumber(value)
        case (key, value: Seq[String]) => key -> JsArray(value.map(JsString))
        case (key, value) => key -> JsString(value.toString)  // Handle other cases as String
      }))
    }
  }
    def main(args: Array[String]): Unit = {
    val kafkaTopic = "tweet-stream"
    val kafkaBroker = "localhost:9092"

    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBroker)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    // Create a KafkaProducer instance
    val kafkaProducer = new KafkaProducer[String, String](kafkaProps)

    val client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

    val filePath = "boulder_flood_geolocated_tweets.json"

    // Import HashtagExtractor
    import config.AppConfig.Extract_hashtags._


    // Function to process each tweet
    def processTweet(line: String): Unit = {
      val json = Json.parse(line)
      json.validate[Tweet] match {
        case JsSuccess(tweet, _) =>
          val hashtags = extractHashtags(tweet.text)

          val tweetWithHashtags = tweet.copy(hashtags = Some(hashtags))
          val updatedJson = Json.toJson(tweetWithHashtags)

          println(s"Tweet from ${tweet.user.screen_name}: ${tweet.text} with hashtags: ${hashtags.mkString(", ")}")

          // Produce the tweet to Kafka
          val record = new ProducerRecord[String, String](kafkaTopic, tweet.id.toString, updatedJson.toString())
          kafkaProducer.send(record)
//        case JsError(errors) =>
//          println(s"Failed to parse tweet: $errors")

          // Store the tweet in Elasticsearch asynchronously using Map
          val tweetMap = Map(
            "created_at" -> tweet.created_at,
            "id" -> tweet.id,
            "text" -> tweet.text,
            "user" -> Json.toJson(tweet.user),
            "hashtags" -> tweet.hashtags.getOrElse(Seq())
          )

          // Convert the Map to JSON using the custom Indexable
          val indexResponse: Future[Response[IndexResponse]] = client.execute {
            indexInto("tweets_index")           // Index name
              .id(tweet.id.toString)            // Document ID
              .doc(tweetMap)                    // Document fields as Map
          }
          // Handle Elasticsearch response
          indexResponse.onComplete {
            case scala.util.Success(response) =>
              println(s"Successfully indexed tweet ${tweet.id} to Elasticsearch.")
            case scala.util.Failure(exception) =>
              println(s"Failed to index tweet ${tweet.id} to Elasticsearch: ${exception.getMessage}")
          }
        case JsError(errors) =>
          println(s"Failed to parse tweet: $errors")
      }
    }


    // Schedule fetching and processing tweets every 3 seconds
    system.scheduler.scheduleAtFixedRate(initialDelay = 0.seconds, interval = 3.seconds) { () =>
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
