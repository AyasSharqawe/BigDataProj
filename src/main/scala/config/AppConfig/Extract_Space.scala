package config.AppConfig

import play.api.libs.json._

object Extract_Space {


  case class Space(latitude: Double, longitude: Double)

  implicit val spaceReads: Reads[Space] = Json.reads[Space]


  implicit val spaceWrites: Writes[Space] = Json.writes[Space]

  def extractSpaces(tweet: JsValue): Option[Seq[Space]] = {

    (tweet \ "geo" \ "coordinates").asOpt[Seq[Double]] match {
      case Some(Seq(lat, lon)) => Some(Seq(Space(lat, lon)))
      case _ => None
    }
  }
}
