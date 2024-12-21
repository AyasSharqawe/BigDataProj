ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProj",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.0",
      "ch.qos.logback" % "logback-classic" % "1.4.11", // for SLF4J logging
      "org.slf4j" % "slf4j-nop" % "2.0.9", // Suppresses the warning
      "com.typesafe.play" %% "play-json" % "2.9.2",
      "com.typesafe.akka" %% "akka-actor-typed" % "2.6.21",
      "org.apache.kafka" % "kafka-clients" % "3.6.0",
      "ch.qos.logback" % "logback-classic" % "1.2.10" // or another 1.2.x version
    ),
    javacOptions := Seq("-source", "11", "-target", "11")  // Add this line to set Java 11
  )
