name := "first_project"

version := "1.0"

scalaVersion := "2.12.18"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "2.12-3.6.1",
  "log4j" % "log4j" % "1.2.17",
  "org.apache.spark" %% "spark-core" % "3.2.0",
  "org.apache.spark" %% "spark-sql" % "3.2.0",
  "org.apache.spark" %% "spark-mllib" % "3.2.0",
  "org.apache.spark" %% "spark-streaming" % "3.2.0",
  "org.twitter4j" % "twitter4j-core" % "4.0.4",
  "org.twitter4j" % "twitter4j-stream" % "4.0.4",
  "org.slf4j" % "slf4j-api" % "1.7.32"
)

