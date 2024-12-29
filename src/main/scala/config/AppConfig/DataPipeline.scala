package config.AppConfig

import com.johnsnowlabs.nlp.annotator._
import com.johnsnowlabs.nlp.base._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import config.AppConfig.Extract_Space.Space
import org.apache.spark.ml.Pipeline
import org.apache.spark.sql.SparkSession // استيراد النوع Space

object DataPipeline {

  // Initialize Spark session
  val spark = SparkSession.builder
    .appName("DataPipeline")
    .master("local[*]")
    .getOrCreate()

  // إعداد Spark NLP
  val documentAssembler = new DocumentAssembler()
    .setInputCol("text")
    .setOutputCol("document")

  val tokenizer = new Tokenizer()
    .setInputCols(Array("document"))
    .setOutputCol("token")

  val sentimentDetector = PretrainedPipeline("analyze_sentiment", lang = "en")

  val ner = NerDLModel.pretrained() // Load pre-trained NER model
    .setInputCols(Array("document", "token"))
    .setOutputCol("ner")

  val nerConverter = new NerConverter()
    .setInputCols(Array("document", "token", "ner"))
    .setOutputCol("ner_span")

  // Define the pipeline
  val pipeline = new Pipeline().setStages(Array(documentAssembler, tokenizer, ner, nerConverter))

  def analyzeSentiment(text: String): String = {
    val result = sentimentDetector.annotate(text)
    result("sentiment").head
  }

  def extractSpaces(text: String): Seq[Space] = {
    val pattern = """(\d+\.\d+),\s*(\d+\.\d+)""".r
    pattern.findAllMatchIn(text).map(m => Space(Seq(m.group(1).toDouble, m.group(2).toDouble))).toSeq
  }

  def processTextData(inputDataPath: String): Unit = {
    val inputData = spark.read.format("csv").option("header", "true").load(inputDataPath)

    // التعامل مع القيم المفقودة
    val cleanedData = inputData.na.fill("Unknown")

    val model = pipeline.fit(cleanedData)
    val processedData = model.transform(cleanedData)

    processedData.show()
  }
}
