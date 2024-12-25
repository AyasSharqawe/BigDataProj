package config.AppConfig
import play.api.libs.json._
object Extract_Space {
  case class Space(coordinates: Seq[Double])

    implicit val spaceReads: Reads[Space] = Json.reads[Space]
    implicit val spaceWrites: Writes[Space] = Json.writes[Space]


}
