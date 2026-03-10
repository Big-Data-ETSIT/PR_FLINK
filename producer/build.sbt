name := "bike-event-producer"
version := "0.1"
scalaVersion := "2.13.12"

libraryDependencies ++= Seq(
  "org.apache.kafka" % "kafka-clients" % "3.5.0",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

run / fork := true
Global / cancelable := true

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
