name := "flink-streaming-analytics"
version := "0.1"
scalaVersion := "2.12.18"

val flinkVersion = "1.17.1"

libraryDependencies ++= Seq(
  "org.apache.flink" %% "flink-streaming-scala" % flinkVersion % "provided",
  "org.apache.flink" % "flink-clients" % flinkVersion % "provided",
  "org.apache.flink" % "flink-connector-kafka" % "3.0.0-1.17",
  "org.json4s" %% "json4s-jackson" % "4.0.6"
)

// make run command include the provided dependencies
Compile / run := Defaults.runTask(Compile / fullClasspath, Compile / run / mainClass, Compile / run / runner).evaluated

// required for Flink 
run / fork := true
javaOptions ++= Seq(
  "--add-opens=java.base/java.lang=ALL-UNNAMED",
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)
Global / cancelable := true

assembly / assemblyMergeStrategy := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case "module-info.class" => MergeStrategy.discard
  case "reference.conf" => MergeStrategy.concat
  case x => MergeStrategy.first
}
