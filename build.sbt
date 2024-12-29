ThisBuild / version := "0.1.0-SNAPSHOT"
ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProj",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.0",
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      "org.slf4j" % "slf4j-nop" % "2.0.9",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "org.apache.spark" %% "spark-core" % "3.0.3",
      "org.apache.spark" %% "spark-sql" % "3.0.3",
      "org.apache.spark" %% "spark-mllib" % "3.0.3",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "3.0.3",

    ),
    javacOptions := Seq("-source", "11", "-target", "11")
  )
