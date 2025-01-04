package config.AppConfig
import com.sksamuel.elastic4s.Indexable
import play.api.libs.json.JsValue


object Implicits {
  implicit val jsValueIndexable: Indexable[JsValue] = new Indexable[JsValue] {
    override def json(t: JsValue): String = t.toString()
  }
}
