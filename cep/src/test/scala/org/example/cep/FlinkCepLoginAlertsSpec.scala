package org.example.cep

import org.apache.flink.streaming.api.scala._
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.BeforeAndAfter
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import java.util.Collections
import scala.collection.JavaConverters._

class FlinkCepLoginAlertsSpec extends AnyFlatSpec with Matchers with BeforeAndAfter {

  val flinkCluster = new MiniClusterWithClientResource(
    new MiniClusterResourceConfiguration.Builder()
      .setNumberSlotsPerTaskManager(2)
      .setNumberTaskManagers(1)
      .build)

  before {
    flinkCluster.before()
    CollectSink.values.clear()
  }

  after {
    flinkCluster.after()
  }

  "FlinkCepLoginAlerts" should "detect 3 consecutive failed logins within 10 seconds" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // Create mock events:
    // User1: Fails at t=1, succeeds at t=2, fails at t=3, fails at t=4 -> Pattern broken, no alert.
    // User2: Fails at t=1, fails at t=5, fails at t=9 -> 3 failures within 10s (8s span) -> ALERT.
    val t0 = 1000000L
    
    val events = Seq(
      LoginEvent("user1", "ip1", t0 + 1000, success = false),
      LoginEvent("user1", "ip1", t0 + 2000, success = true),
      LoginEvent("user1", "ip1", t0 + 3000, success = false),
      LoginEvent("user1", "ip1", t0 + 4000, success = false),

      LoginEvent("user2", "ip2", t0 + 1000, success = false),
      LoginEvent("user2", "ip2", t0 + 5000, success = false),
      LoginEvent("user2", "ip2", t0 + 9000, success = false)
    )

    val loginStream = env.fromCollection(events)

    val alertStream = FlinkCepLoginAlerts.processLoginEvents(loginStream)
    
    alertStream.addSink(new CollectSink)

    env.execute("CEP Test Job")

    val alerts = CollectSink.values.asScala.toList

    alerts should have size 1
    alerts.head.userId shouldEqual "user2"
    alerts.head.reason should include (s"${t0 + 1000}")
    alerts.head.reason should include (s"${t0 + 9000}")
  }

  it should "not detect failures spanning more than 10 seconds" in {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val t0 = 1000000L
    
    val events = Seq(
      LoginEvent("user3", "ip3", t0 + 1000, success = false),
      LoginEvent("user3", "ip3", t0 + 5000, success = false),
      LoginEvent("user3", "ip3", t0 + 12000, success = false) // Third failure is 11 seconds after first
    )

    val loginStream = env.fromCollection(events)

    val alertStream = FlinkCepLoginAlerts.processLoginEvents(loginStream)
    
    alertStream.addSink(new CollectSink)

    env.execute("CEP Test Job 2")

    val alerts = CollectSink.values.asScala.toList

    alerts shouldBe empty
  }
}

// Global sink to collect results for test verification
class CollectSink extends SinkFunction[AlertEvent] {
  override def invoke(value: AlertEvent, context: SinkFunction.Context): Unit = {
    CollectSink.values.add(value)
  }
}

object CollectSink {
  // Must be thread-safe for Flink
  val values: java.util.List[AlertEvent] = Collections.synchronizedList(new java.util.ArrayList[AlertEvent]())
}
