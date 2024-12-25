package config.AppConfig

object Extract_hashtags {

   def extractHashtags(text: String): Seq[String] = {
      val hash= """#\w+""".r
      hash.findAllIn(text).toSeq
    }


}
