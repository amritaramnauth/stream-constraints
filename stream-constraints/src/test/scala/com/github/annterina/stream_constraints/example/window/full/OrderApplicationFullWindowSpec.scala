package com.github.annterina.stream_constraints.example.window.full

import java.time.{Duration, Instant}
import java.util.Properties
import java.util.Date

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderApplicationFullWindowSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val cancelledUpdatedWindow = new WindowConstraintBuilder[String, OrderEvent]
      .before((_, e) => e.action == "CANCELLED", "Order Cancelled")
      .after((_, e) => e.action == "UPDATED", "Order Updated")
      .window(Duration.ofSeconds(10))
      .swap

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .windowConstraint(cancelledUpdatedWindow).withFullWindows()
      .link((_, e) => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream("orders")(Consumed.`with`(Serdes.String, orderEventSerde))
      .constrain(constraints)
      .to("orders-output-topic")(Produced.`with`(Serdes.String, orderEventSerde))

    testDriver = new TopologyTestDriver(builder.build(), config)

    inputTopic = testDriver.createInputTopic(
      "orders",
      Serdes.String.serializer(),
      OrderEventSerde.serializer()
    )

    outputTopic = testDriver.createOutputTopic(
      "orders-output-topic",
      Serdes.String.deserializer(),
      OrderEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Application with a full window constraint") {

    it("should accept after events for the whole window") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CANCELLED"), timestamp)
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(2))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(4))
      inputTopic.pipeInput("000", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(6))

      // stream advances to close the window
      inputTopic.pipeInput("111", OrderEvent(1, new Date("1660931536"), "customer1", "NOT_RELATED"), timestamp.plusSeconds(11))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "789")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "000")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")

      // read the message that advances stream time
      outputTopic.readKeyValue()

      val nextOutput = outputTopic.readKeyValue()

      assert(nextOutput.key == "123")
      assert(nextOutput.value.key == 1)
      assert(nextOutput.value.action == "CANCELLED")
    }

    it("should accept multiple before events and after events for the whole window") {
      val timestamp = Instant.parse("2021-03-21T10:15:00.00Z")
      inputTopic.pipeInput("000", OrderEvent(1, new Date("1660931536"), "customer1", "CANCELLED"), timestamp)
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CANCELLED"), timestamp.plusSeconds(1))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(2))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(4))
      inputTopic.pipeInput("111", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"), timestamp.plusSeconds(6))

      // stream advances to close the window
      inputTopic.pipeInput("222", OrderEvent(1, new Date("1660931536"), "customer1", "NOT_RELATED"), timestamp.plusSeconds(11))

      val output = outputTopic.readKeyValue()

      assert(output.key == "456")
      assert(output.value.key == 1)
      assert(output.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "789")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "111")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")

      // read the message that advances stream time
      outputTopic.readKeyValue()

      val firstBefore = outputTopic.readKeyValue()

      assert(firstBefore.key == "000")
      assert(firstBefore.value.key == 1)
      assert(firstBefore.value.action == "CANCELLED")

      val secondBefore = outputTopic.readKeyValue()

      assert(secondBefore.key == "123")
      assert(secondBefore.value.key == 1)
      assert(secondBefore.value.action == "CANCELLED")
    }
  }
}
