package config.AppConfig

import config.AppConfig.Sentiment_Analyzing._

object TrainAndSaveModelApp {
  def main(args: Array[String]): Unit = {
    val trainingDataPath = "C:/BigDataProj/data/tableConvert.com_xajo0f.csv"
    val modelSavePath = "C:/BigDataProj/data/trained_model"

    trainAndSaveModel(trainingDataPath, modelSavePath)
    println(s"Model has been trained and saved to $modelSavePath")
  }
}
