package org.example.cep

import java.util.Properties
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import io.circe.syntax._
import io.circe.generic.auto._
import scala.util.Random

object FakeKafkaSource {

  val TOPIC = "login-events"
  val BOOTSTRAP_SERVERS = "localhost:9092" // Assuming we run from IDE connecting to dockerized Kafka

  def main(args: Array[String]): Unit = {
    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    
    val random = new Random()
    val users = Seq("user_alice", "user_bob", "user_charlie", "user_dave")
    
    println(s"Starting fake login event generator. Publishing to $TOPIC...")
    println("Press Ctrl+C to stop.")
    
    try {
      while (true) {
        val user = users(random.nextInt(users.length))
        
        // We artificially inject consecutive failures for user_bob occasionally
        val (userId, success) = if (random.nextDouble() < 0.15) {
          ("user_bob", false)
        } else {
          (user, random.nextDouble() > 0.3) // 70% chance of success
        }

        val event = LoginEvent(
          userId = userId,
          ipAddress = s"192.168.1.${random.nextInt(255)}",
          timestamp = System.currentTimeMillis(),
          success = success
        )
        
        val record = new ProducerRecord[String, String](TOPIC, event.userId, event.asJson.noSpaces)
        producer.send(record)
        
        println(s"Sent: ${event.asJson.noSpaces}")
        
        // Sleep to throttle events. When simulating failed login bursts for user_bob, sleep less.
        Thread.sleep(if (userId == "user_bob" && !success) 200 else 1000)
      }
    } catch {
      case e: Exception => e.printStackTrace()
    } finally {
      producer.close()
    }
  }
}
