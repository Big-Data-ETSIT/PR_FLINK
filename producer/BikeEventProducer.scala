//> using scala "2.13.12"
//> using dep "org.apache.kafka:kafka-clients:3.5.0"
//> using dep "org.json4s::json4s-jackson:4.0.6"

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import org.apache.kafka.common.serialization.StringSerializer
import java.util.Properties
import scala.util.Random

object BikeEventProducer {
  def main(args: Array[String]): Unit = {
    val props = new Properties()
    // Puerto externo configurado en el docker-compose
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)

    val producer = new KafkaProducer[String, String](props)
    val topic = "bike-events"
    val random = new Random()

    println("Generando eventos de prueba... [Ctrl+C para salir]")

    while (true) {
      val bikeId = s"bike_${random.nextInt(10)}"
      val stationId = s"station_${random.nextInt(5)}"
      val userId = s"user_${random.nextInt(100)}"
      val eventType = if (random.nextDouble() > 0.4) "ride_start" else "ride_end"
      val timestamp = System.currentTimeMillis()

      val json = s"""{"bike_id":"$bikeId","station_id":"$stationId","user_id":"$userId","event_type":"$eventType","timestamp":$timestamp}"""
      
      val record = new ProducerRecord[String, String](topic, bikeId, json)
      producer.send(record)
      
      println(s"Enviado: $json")
      Thread.sleep(1000)
    }
  }
}
