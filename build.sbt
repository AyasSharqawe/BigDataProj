ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProj",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.0",
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
      "ch.qos.logback" % "logback-classic" % "1.2.10",
      "org.apache.spark" %% "spark-core" % "3.0.3",
      "org.apache.spark" %% "spark-sql" % "3.0.3",
      "org.apache.spark" %% "spark-mllib" % "3.3.2",
      "com.sksamuel.elastic4s" %% "elastic4s-core" % "7.16.0",
      "com.sksamuel.elastic4s" %% "elastic4s-client-esjava" % "7.17.3",
      "com.johnsnowlabs.nlp" %% "spark-nlp" % "4.0.0",
      "io.circe" %% "circe-core" % "0.14.3",
      "io.circe" %% "circe-generic" % "0.14.3",
      "io.circe" %% "circe-parser" % "0.14.3",
      "com.typesafe.play" %% "play" % "2.8.8",  // Play Framework
      "com.sksamuel.elastic4s" %% "elastic4s-http" % "6.5.0", // Elasticsearch client
      "com.typesafe.akka" %% "akka-actor" % "2.6.21",
      "com.typesafe.akka" %% "akka-stream" % "2.6.21",
      "com.typesafe.akka" %% "akka-serialization-jackson" % "2.6.21",
      "com.typesafe.akka" %% "akka-protobuf-v3" % "2.6.21",
      "org.scalaj" % "scalaj-http_2.13" % "2.4.2",
      "com.typesafe.play" %% "play-ws" % "2.7.4",

      // Additional dependencies can be added below if needed.
      "org.apache.kafka" % "kafka-clients" % "2.7.0",
      "com.sksamuel.elastic4s" %% "elastic4s-http" % "6.7.0"
    ),
    javacOptions := Seq("-source", "11", "-target", "11"),
    Global / usePipelining := false
  )
