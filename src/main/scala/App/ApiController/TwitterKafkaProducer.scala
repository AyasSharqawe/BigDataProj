package App.ApiController
import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object TwitterKafkaProducerApp {
  def main(args: Array[String]): Unit = {



    // Twitter API credentials
    val consumerKey = "N3uhWhwUvNQBSdY5JhrcK0Jpg"
    val consumerSecret = "Gj8FLQjSJJziRkXfCxtXBkF1nuPUYTStz4BMNyGRKC9I4EoEPh"
    val accessToken = "1869792054798057473-E2ap9sTnQRUX0h1CzW4TUO8gvW6y46"
    val accessTokenSecret = "nuhsl2pOxCj7IY4ZpymIDAEtFMwlQsmlPczB8E16TgMhW"


    // Set up the configuration
    val cb = new ConfigurationBuilder()
    cb.setOAuthConsumerKey(consumerKey)
      .setOAuthConsumerSecret(consumerSecret)
      .setOAuthAccessToken(accessToken)
      .setOAuthAccessTokenSecret(accessTokenSecret)


    // Initialize Twitter instance
    val twitter = new TwitterFactory(cb.build()).getInstance()


    try {
      val user = twitter.verifyCredentials()
      println(s"Successfully connected to Twitter as ${user.getName}")
    } catch {
      case e: TwitterException =>
        println(s"Failed to connect to Twitter: ${e.getMessage}")


    }
  }
}
