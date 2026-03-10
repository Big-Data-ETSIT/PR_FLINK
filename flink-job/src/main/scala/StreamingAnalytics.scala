
import org.apache.flink.streaming.api.scala._
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.eventtime.WatermarkStrategy

case class BikeEvent(
  bike_id: String,
  station_id: String,
  user_id: String,
  event_type: String,
  timestamp: Long
)

object StreamingAnalytics {

  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // TODO 1: Create KafkaSource

    // TODO 2: Create stream from source

    // TODO 3: Parse JSON to BikeEvent

    // TODO 4: Filter ride_start events

    // TODO 5: Key by station

    // TODO 6: Apply 30s tumbling window and count

    // TODO 7: Print results

    env.execute("Bike Streaming Analytics")
  }
}

