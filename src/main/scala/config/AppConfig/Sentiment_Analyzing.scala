package config.AppConfig

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline

object Sentiment_Analyzing {
  val sentimentDetector = PretrainedPipeline("analyze_sentiment", lang = "en")

  def analyzeSentiment(text: String): String = {
    val result = sentimentDetector.annotate(text)
    result("sentiment").head
  }
}
