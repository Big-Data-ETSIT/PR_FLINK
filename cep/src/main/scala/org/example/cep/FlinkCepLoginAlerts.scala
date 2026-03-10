package org.example.cep

import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala._
import org.apache.flink.cep.CEP
import org.apache.flink.cep.pattern.Pattern
import org.apache.flink.cep.pattern.conditions.SimpleCondition
import org.apache.flink.cep.PatternSelectFunction
import java.util.{Map => JMap, List => JList}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.java.utils.ParameterTool
import java.time.Duration

import io.circe.parser._
import io.circe.generic.auto._

case class LoginEvent(userId: String, ipAddress: String, timestamp: Long, success: Boolean)
case class AlertEvent(userId: String, reason: String)

object FlinkCepLoginAlerts {

  def main(args: Array[String]): Unit = {
    val params = ParameterTool.fromArgs(args)
    // Default to kafka:29092 for docker, but you can override via --bootstrap.servers localhost:9092 for IDE
    val bootstrapServers = params.get("bootstrap.servers", "kafka:29092")
    val topic = params.get("topic", "login-events")

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    
    val kafkaSource = KafkaSource.builder[String]()
      .setBootstrapServers(bootstrapServers)
      .setTopics(topic)
      .setGroupId("flink-cep-group")
      .setStartingOffsets(OffsetsInitializer.latest())
      .setValueOnlyDeserializer(new SimpleStringSchema())
      .build()

    // Consume raw JSON strings from Kafka
    val rawStream = env.fromSource(
      kafkaSource, 
      WatermarkStrategy.noWatermarks[String](), 
      "Kafka String Source"
    )

    // Parse JSON strings to LoginEvent
    val loginStream = rawStream.flatMap { jsonStr =>
      decode[LoginEvent](jsonStr) match {
        case Right(event) => Some(event)
        case Left(error) => 
          println(s"Failed to parse event: $jsonStr, err: $error")
          None
      }
    }

    val alertsStream = processLoginEvents(loginStream)

    // Print to standard out.
    // When running in Docker, you can see this in TaskManager logs.
    alertsStream.print("ALERT")

    env.execute("Flink CEP Login Alerts on Docker")
  }

  /**
   * Core CEP logic extracted for testability without relying on Kafka.
   */
  def processLoginEvents(loginStream: DataStream[LoginEvent]): DataStream[AlertEvent] = {
    // Assign timestamps for CEP
    val timestampedStream = loginStream.assignTimestampsAndWatermarks(
      WatermarkStrategy
        .forBoundedOutOfOrderness[LoginEvent](Duration.ofSeconds(2))
        .withTimestampAssigner(new SerializableTimestampAssigner[LoginEvent] {
          override def extractTimestamp(element: LoginEvent, recordTimestamp: Long): Long = element.timestamp
        })
    )

    // Key by user to apply CEP pattern per user
    val keyedStream = timestampedStream.keyBy(_.userId)

    // Define the CEP Pattern: 3 consecutive failures within 10 seconds using Java API
    val pattern = Pattern.begin[LoginEvent]("first")
      .where(new SimpleCondition[LoginEvent] {
        override def filter(value: LoginEvent): Boolean = !value.success
      })
      .next("second")
      .where(new SimpleCondition[LoginEvent] {
        override def filter(value: LoginEvent): Boolean = !value.success
      })
      .next("third")
      .where(new SimpleCondition[LoginEvent] {
        override def filter(value: LoginEvent): Boolean = !value.success
      })
      .within(Time.seconds(10))

    val patternStreamJava = CEP.pattern(keyedStream.javaStream, pattern)

    // Select the matched patterns and create an AlertEvent
    val alertsStreamJava = patternStreamJava.select(new PatternSelectFunction[LoginEvent, AlertEvent] {
      override def select(patternMap: JMap[String, JList[LoginEvent]]): AlertEvent = {
        val first = patternMap.get("first").get(0)
        val third = patternMap.get("third").get(0)
        
        AlertEvent(
          userId = first.userId,
          reason = s"3 consecutive failed logins detected between ${first.timestamp} and ${third.timestamp}"
        )
      }
    })

    new org.apache.flink.streaming.api.scala.DataStream(alertsStreamJava)
  }
}
