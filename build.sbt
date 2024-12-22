ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "BigDataProj",
    libraryDependencies ++= Seq(
      "org.apache.kafka" %% "kafka" % "2.8.0",
      "ch.qos.logback" % "logback-classic" % "1.4.11",
      "org.slf4j" % "slf4j-nop" % "2.0.9" ,
      "org.twitter4j" % "twitter4j-core" % "4.0.7",
      "org.twitter4j" % "twitter4j-stream" % "3.0.3"



    )
  )
