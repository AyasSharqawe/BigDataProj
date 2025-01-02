package App.ApiController

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Response, Indexable, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.indexes.{CreateIndexResponse, IndexResponse}
import config.AppConfig.Extract_Space._
import config.AppConfig.Sentiment_Analyzing._
import config.AppConfig.DataPipeline._
import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.Encoder
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.{KafkaConsumer}
import play.api.libs.json._
import config.AppConfig.Extract_hashtags._
import com.sksamuel.elastic4s.ElasticDsl._
import java.util.Properties
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source

case class User(name: String, screen_name: String, location: Option[String])
case class Tweet(created_at: String, id: Long, text: String, user: User, hashtags: Option[Seq[String]], space: Option[Seq[Space]])

object TwitterKafkaProducer {

  implicit val system: ActorSystem = ActorSystem("TwitterStreamSimulator")

  println("Initializing SparkSession...")
  implicit val spark: SparkSession = SparkSession.builder
    .appName("TwitterKafkaProducer")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()
  println("SparkSession initialized.")

  import spark.implicits._

  implicit val tweetEncoder: Encoder[Tweet] = Encoders.product[Tweet]

  implicit val userFormat: Format[User] = Json.format[User]
  implicit val tweetFormat: Format[Tweet] = Json.format[Tweet]

  implicit val mapWrites: Writes[Map[String, Any]] = (map: Map[String, Any]) => {
    Json.obj(map.map {
      case (key, value) =>
        val jsonValue: Json.JsValueWrapper = value match {
          case v: String => JsString(v)
          case v: Int => JsNumber(v)
          case v: Long => JsNumber(v)
          case v: Double => JsNumber(v)
          case v: Float => JsNumber(BigDecimal(v.toString))
          case v: Boolean => JsBoolean(v)
          case v: Seq[_] => JsArray(v.map {
            case s: String => JsString(s)
            case i: Int => JsNumber(i)
            case l: Long => JsNumber(l)
            case d: Double => JsNumber(d)
            case f: Float => JsNumber(BigDecimal(f.toString))
            case b: Boolean => JsBoolean(b)
            case _ => JsNull
          })
          case _ => JsNull
        }
        key -> jsonValue
    }.toSeq: _*)
  }

  implicit object MapIndexable extends Indexable[Map[String, Any]] {
    override def json(map: Map[String, Any]): String = {
      Json.stringify(Json.toJson(map)(mapWrites))
    }
  }

  def main(args: Array[String]): Unit = {
    try {
      val kafkaTopic = "tweet-stream"
      val kafkaBroker = "localhost:9092"

      println("Initializing KafkaProducer...")
      val kafkaProps = new Properties()
      kafkaProps.put("bootstrap.servers", kafkaBroker)
      kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
      val kafkaProducer = new KafkaProducer[String, String](kafkaProps)
      println("KafkaProducer initialized.")

      println("Initializing ElasticClient...")
      val client = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))
      println("ElasticClient initialized.")

      // إضافة الكود الخاص بإنشاء الفهرس هنا
      val createIndexResponse: Future[Response[CreateIndexResponse]] = client.execute {
        createIndex("tweets_index")
      }

      createIndexResponse.onComplete {
        case scala.util.Success(response) =>
          response match {
            case RequestSuccess(_, _, _, result) =>
              println(s"تم إنشاء الفهرس tweets_index بنجاح: ${result.acknowledged}")
            case RequestFailure(_, _, _, error) =>
              println(s"فشل إنشاء الفهرس tweets_index: ${error.reason}")
          }
        case scala.util.Failure(exception) =>
          println(s"فشل إنشاء الفهرس tweets_index: ${exception.getMessage}")
      }

      val filePath = "boulder_flood_geolocated_tweets.json"

      def sendToKafka(): Unit = {
        println("Reading data from file and sending to Kafka...")
        val tweetSource = Source.fromFile(filePath)
        try {
          val lines = tweetSource.getLines().toList
          println(s"Found ${lines.length} lines in the file.")
          lines.foreach { line =>
            kafkaProducer.send(new ProducerRecord[String, String](kafkaTopic, line))
          }
        } catch {
          case e: Exception => println(s"Error reading file: ${e.getMessage}")
        } finally {
          tweetSource.close()
        }
      }

      def processTweetsFromKafka(): Unit = {
        println("Initializing KafkaConsumer...")
        val consumerProps = new Properties()
        consumerProps.put("bootstrap.servers", kafkaBroker)
        consumerProps.put("group.id", "twitter-consumer-group")
        consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
        consumerProps.put("auto.offset.reset", "earliest")

        val kafkaConsumer = new KafkaConsumer[String, String](consumerProps)
        kafkaConsumer.subscribe(java.util.Collections.singletonList(kafkaTopic))
        println("KafkaConsumer initialized.")

        println("Consuming tweets from Kafka...")
        while (true) {
          val records = kafkaConsumer.poll(1000)
          val iterator = records.iterator()
          while (iterator.hasNext) {
            val record = iterator.next()
            val tweetJson = record.value()
            processTweet(tweetJson)
          }
        }
      }

      def processTweet(line: String): Unit = {
        try {
          println(s"Processing tweet: $line")
          val json = Json.parse(line)
          json.asOpt[Tweet] match {
            case Some(tweet) =>
              println(s"Parsed tweet: $tweet")
              val hashtags = extractHashtags(tweet.text)
              val spaces = extractSpaces(Json.toJson(tweet))
              val sentiment = analyzeSentiment(tweet.text)

              val tweetWithMetadata = tweet.copy(
                hashtags = Some(hashtags),
                space = Some(spaces.getOrElse(Seq.empty))
              )

              println(s"Tweet with metadata: $tweetWithMetadata")

              val tweetDf = Seq(tweetWithMetadata).toDF()
              val processedTweetDf = pipeline.fit(tweetDf).transform(tweetDf)
              val processedTweet = processedTweetDf.as[Tweet].collect().head

              val tweetMap = Map(
                "created_at" -> processedTweet.created_at,
                "id" -> processedTweet.id,
                "text" -> processedTweet.text,
                "user" -> Json.stringify(Json.toJson(processedTweet.user)),
                "hashtags" -> processedTweet.hashtags.getOrElse(Seq.empty),
                "space" -> processedTweet.space.getOrElse(Seq.empty),
                "sentiment" -> sentiment
              )

              // إرسال التغريدة إلى Elasticsearch
              val indexResponse: Future[Response[IndexResponse]] = client.execute {
                indexInto("tweets_index").id(tweet.id.toString).doc(tweetMap)
              }

              indexResponse.onComplete {
                case scala.util.Success(response) =>
                  response match {
                    case RequestSuccess(_, _, _, result) =>
                      println(s"تم تخزين التغريدة ${tweet.id} في Elasticsearch بنجاح. الحالة: ${result.toString}")
                    case RequestFailure(_, _, _, error) =>
                      println(s"فشل تخزين التغريدة ${tweet.id} في Elasticsearch: ${error.reason}")
                  }
                case scala.util.Failure(exception) =>
                  println(s"فشل تخزين التغريدة ${tweet.id} في Elasticsearch: ${exception.getMessage}")
              }

            case None =>
              println(s"Failed to parse tweet")
          }
        } catch {
          case e: Exception =>
            println(s"Exception: ${e.getMessage}")
        }
      }

      // Start the data pipeline
      sendToKafka()
      processTweetsFromKafka()

    } catch {
      case e: Exception =>
        println(s"Error in main: ${e.getMessage}")
    }
  }
}
