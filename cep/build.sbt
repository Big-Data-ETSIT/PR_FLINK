name := "flink-scala-examples"
version := "0.1"
scalaVersion := "2.12.18"

val flinkVersion = "1.17.2"

// Flink dependencies
// We do not use "provided" here so that we can run the examples directly
// from sbt using `sbt run` or `sbt "runMain ..."` without any extra configuration.
libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-scala"           % flinkVersion,
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion,
  "org.apache.flink" % "flink-clients"          % flinkVersion,
  "org.apache.flink" % "flink-connector-kafka"  % "3.0.1-1.17", // Flink Kafka Connector
  "org.apache.flink" % "flink-cep"             % flinkVersion, // Flink CEP
  
  // JSON Parsing
  "io.circe" %% "circe-core" % "0.14.3",
  "io.circe" %% "circe-generic" % "0.14.3",
  "io.circe" %% "circe-parser" % "0.14.3",
  
  // Logging for SLF4J (Kafka client needs it)
  "org.slf4j" % "slf4j-simple" % "2.0.7",

  // Testing
  "org.scalatest" %% "scalatest" % "3.2.16" % Test,
  "org.apache.flink" % "flink-test-utils" % flinkVersion % Test
)

// Add JVM options for Flink testing on Java 17
Test / javaOptions ++= Seq(
  "--add-opens", "java.base/java.lang=ALL-UNNAMED",
  "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED",
  "--add-opens", "java.base/java.util=ALL-UNNAMED"
)
Test / fork := true

// Assembly merge strategy to handle duplicate files in FAT JAR
ThisBuild / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}
