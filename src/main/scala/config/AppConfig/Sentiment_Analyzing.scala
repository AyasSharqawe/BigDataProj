package config.AppConfig

import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.ml.feature.{Tokenizer, HashingTF, IDF}
import org.apache.spark.ml.classification.LogisticRegression
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Sentiment_Analyzing {

  val spark = SparkSession.builder
    .appName("SentimentAnalysisPipeline")
    .master("local[*]") // If running locally
    .getOrCreate()


  val tokenizer = new Tokenizer().setInputCol("text").setOutputCol("words")
  val hashingTF = new HashingTF().setInputCol("words").setOutputCol("rawFeatures")
  val idf = new IDF().setInputCol("rawFeatures").setOutputCol("features")
  val lr = new LogisticRegression().setMaxIter(10).setRegParam(0.001)

  val pipeline = new Pipeline().setStages(Array(tokenizer, hashingTF, idf, lr))

  def trainAndSaveModel(trainingDataPath: String, modelSavePath: String): Unit = {
    val trainingData = spark.read.format("csv").option("header", "true").load(trainingDataPath)
      .withColumnRenamed("Tweet", "text")
      .withColumn("label", when(col("Sentiment") === "positive", 1.0)
        .when(col("Sentiment") === "negative", 0.0)
        .otherwise(2.0))

    val model = pipeline.fit(trainingData)


    try {
      model.write.overwrite().save(modelSavePath)
      println(s"Model has been successfully saved to $modelSavePath")
    } catch {
      case e: Exception =>
        println(s"Error occurred while saving the model: ${e.getMessage}")
    }
  }


  def loadSentimentModel(modelPath: String): PipelineModel = {
    try {
      PipelineModel.load(modelPath)
    } catch {
      case e: Exception =>
        println(s"Error occurred while loading the model: ${e.getMessage}")
        throw e
    }
  }}
