package App.ApiController

import akka.actor.ActorSystem
import com.sksamuel.elastic4s.{ElasticClient, ElasticProperties, Response, RequestFailure, RequestSuccess}
import com.sksamuel.elastic4s.http.JavaClient
import com.sksamuel.elastic4s.requests.indexes.{CreateIndexResponse, IndexResponse}
import com.sksamuel.elastic4s.ElasticDsl._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.clients.consumer.KafkaConsumer
import play.api.libs.json._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import java.util.Properties
import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.sql.SparkSession
import config.AppConfig.DataPipeline._
import config.AppConfig.Extract_hashtags._
import config.AppConfig.Sentiment_Analyzing._
import config.AppConfig.Extract_Space._
import config.AppConfig.Implicits._

case class User(name: String, screen_name: String, location: Option[String])
case class Space(id: String, name: String, location: Option[String])
case class Tweet(created_at: String, id: Long, text: String, user: User, hashtags: Option[Seq[String]], space: Option[Seq[Space]])

object TwitterKafkaProducer {
  Logger.getLogger("org.apache.kafka.clients").setLevel(Level.ERROR)
  implicit val system: ActorSystem = ActorSystem("TwitterStreamSimulator")

  val kafkaTopic = "tweet-stream"
  val kafkaBroker = "localhost:9092"
  val elasticClient = ElasticClient(JavaClient(ElasticProperties("http://localhost:9200")))

  implicit val spark: SparkSession = SparkSession.builder
    .appName("TwitterKafkaProducer")
    .master("local[*]")
    .config("spark.driver.memory", "4g")
    .config("spark.executor.memory", "2g")
    .config("spark.testing.memory", "2147480000")
    .getOrCreate()

  import spark.implicits._

  implicit val userFormat: Format[User] = Json.format[User]
  implicit val spaceFormat: Format[Space] = Json.format[Space]
  implicit val tweetFormat: Format[Tweet] = Json.format[Tweet]

  def main(args: Array[String]): Unit = {
    try {
      testKafkaConnection()
      testElasticsearchConnection()
      createIndexIfNotExists()
      sendToKafka("boulder_flood_geolocated_tweets.json")
      processTweetsFromKafka()
    } catch {
      case e: Exception =>
        println(s"Error in main: ${e.getMessage}")
    }
  }

  def testKafkaConnection(): Unit = {
    try {
      val producer = createKafkaProducer()
      producer.send(new ProducerRecord[String, String](kafkaTopic, "Test Message"))
      println("Successfully connected to Kafka.")
      producer.close()
    } catch {
      case e: Exception =>
        println(s"Error connecting to Kafka: ${e.getMessage}")
    }
  }

  def testElasticsearchConnection(): Unit = {
    val response: Future[Response[CreateIndexResponse]] = elasticClient.execute {
      createIndex("test_index")
    }

    response.onComplete {
      case scala.util.Success(_) => println("Successfully connected to Elasticsearch.")
      case scala.util.Failure(exception) =>
        println(s"Error connecting to Elasticsearch: ${exception.getMessage}")
    }
  }

  def createIndexIfNotExists(): Unit = {
    elasticClient.execute {
      createIndex("tweets_index")
    }.onComplete {
      case scala.util.Success(response) =>
        println("Index created successfully.")
      case scala.util.Failure(exception) =>
        println(s"Failed to create index: ${exception.getMessage}")
    }
  }

  def sendToKafka(filePath: String): Unit = {
    println("Reading data from file and sending to Kafka...")
    val kafkaProducer = createKafkaProducer()

    try {
      val tweetSource = Source.fromFile(filePath)
      val lines = tweetSource.getLines().toList
      println(s"Found ${lines.length} lines in the file.")
      lines.foreach { line =>
        kafkaProducer.send(new ProducerRecord[String, String](kafkaTopic, line))
      }
      tweetSource.close()
    } catch {
      case _: java.io.FileNotFoundException =>
        println(s"File $filePath not found. Please check the path.")
      case e: Exception =>
        println(s"Error while reading the file: ${e.getMessage}")
    }
  }

  def createKafkaProducer(): KafkaProducer[String, String] = {
    println("Initializing KafkaProducer...")
    val kafkaProps = new Properties()
    kafkaProps.put("bootstrap.servers", kafkaBroker)
    kafkaProps.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    kafkaProps.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    new KafkaProducer[String, String](kafkaProps)
  }

  def processTweetsFromKafka(): Unit = {
    println("Initializing KafkaConsumer...")
    val kafkaConsumer = createKafkaConsumer()
    kafkaConsumer.subscribe(java.util.Collections.singletonList(kafkaTopic))
    println("KafkaConsumer is ready.")

    println("Consuming tweets from Kafka...")
    while (true) {
      val records = kafkaConsumer.poll(1000)
      records.forEach { record =>
        val tweetJson = record.value()
        processTweet(tweetJson)
      }
    }
  }

  def createKafkaConsumer(): KafkaConsumer[String, String] = {
    val consumerProps = new Properties()
    consumerProps.put("bootstrap.servers", kafkaBroker)
    consumerProps.put("group.id", "twitter-consumer-group")
    consumerProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
    consumerProps.put("auto.offset.reset", "earliest")
    new KafkaConsumer[String, String](consumerProps)
  }

  def processTweet(line: String): Unit = {
    try {
      println(s"Processing tweet: $line")
      val json = Json.parse(line)
      json.asOpt[Tweet] match {
        case Some(tweet) =>
          println(s"Parsed tweet: $tweet")

          val tweetDf = Seq(tweet).toDF()
          val processedTweetDf = pipeline.fit(tweetDf).transform(tweetDf)

          val processedTweet = processedTweetDf.as[Tweet].collect().head

          val hashtags = extractHashtags(processedTweet.text)
          val spaces = extractSpaces(Json.toJson(processedTweet))
          val sentiment = analyzeSentiment(processedTweet.text)

          val tweetWithMetadata = processedTweet.copy(
            hashtags = Some(hashtags),
            space = Some(spaces.getOrElse(Seq.empty).map(_.asInstanceOf[Space]))
          )

          saveTweetToElasticsearch(tweetWithMetadata, sentiment)

        case None =>
          println(s"Failed to parse tweet")
      }
    } catch {
      case e: Exception =>
        println(s"Exception: ${e.getMessage}")
    }
  }

  def saveTweetToElasticsearch(tweet: Tweet, sentiment: String): Unit = {
    val tweetMap = Map[String, JsValue](
      "created_at" -> JsString(tweet.created_at),
      "id" -> JsNumber(tweet.id),
      "text" -> JsString(tweet.text),
      "user" -> Json.toJson(tweet.user),
      "hashtags" -> JsArray(tweet.hashtags.getOrElse(Seq.empty).map(JsString)),
      "space" -> JsArray(tweet.space.getOrElse(Seq.empty).map(space => Json.toJson(space))),
      "sentiment" -> JsString(sentiment)
    )

    val tweetJson = Json.toJson(tweetMap)

    val indexResponse: Future[Response[IndexResponse]] = elasticClient.execute {
      indexInto("tweets_index").id(tweet.id.toString).doc(tweetJson)
    }

    indexResponse.onComplete {
      case scala.util.Success(response) =>
        response match {
          case RequestSuccess(_, _, _, result) =>
            println(s"Tweet ${tweet.id} stored successfully in Elasticsearch. Status: ${result.toString}")
          case RequestFailure(_, _, _, error) =>
            println(s"Failed to store tweet ${tweet.id} in Elasticsearch: ${error.reason}")
        }
      case scala.util.Failure(exception) =>
        println(s"Failed to store tweet ${tweet.id} in Elasticsearch: ${exception.getMessage}")
    }
  }
}
