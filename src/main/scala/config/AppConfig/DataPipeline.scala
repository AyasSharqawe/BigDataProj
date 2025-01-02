package config.AppConfig
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.embeddings.WordEmbeddingsModel
import org.apache.spark.ml.Pipeline

object DataPipeline {

  val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val tokenizer = new Tokenizer()
    .setInputCols(Array("document"))
    .setOutputCol("token")

  val wordEmbeddings = WordEmbeddingsModel.pretrained("glove_100d")
    .setInputCols(Array("document", "token"))
    .setOutputCol("word_embeddings")

  val sentimentDetector = ViveknSentimentModel.pretrained()
    .setInputCols(Array("document", "token"))
    .setOutputCol("sentiment")

  val ner = NerDLModel.pretrained()
    .setInputCols(Array("document", "token", "word_embeddings"))
    .setOutputCol("ner")

  val nerConverter = new NerConverter()
    .setInputCols(Array("document", "token", "ner"))
    .setOutputCol("ner_span")

  val pipeline: Pipeline = new Pipeline().setStages(Array(
    documentAssembler, tokenizer, wordEmbeddings, sentimentDetector, ner, nerConverter
  ))

  println("Pipeline has been successfully created.")
}
