package com.github.annterina.stream_constraints.example.prerequisites

import java.util.Properties
import java.util.Date

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderApplicationSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val constraint = new ConstraintBuilder[String, OrderEvent, Integer]
      .prerequisite(((_, e) => e.action == "CREATED", "Order Created"),
        ((_, e) => e.action == "UPDATED", "Order Updated"))
      .link((_, e) => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

    val builder = new CStreamsBuilder()

    builder
      .stream("orders")(Consumed.`with`(Serdes.String, orderEventSerde))
      .constrain(constraint)
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

  describe("Order Application with single prerequisite constraint") {

    it("should emit the prerequisite event") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 1)
      assert(output.value.action == "CREATED")
    }

    it("should buffer an event when the prerequisite was not processed") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))

      assert(outputTopic.isEmpty)
    }

    it("should buffer an event when the prerequisite was not processed and publish not related event") {
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(2, new Date("1660931536"), "customer1", "CREATED"))

      val output = outputTopic.readKeyValue()

      assert(output.key == "123")
      assert(output.value.key == 2)
      assert(output.value.action == "CREATED")
    }

    it("should publish both events after receiving the prerequisite") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "456")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "123")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")
    }

    it("should accept and publish multiple prerequisite events") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CREATED")
    }

    it("should publish prerequisite event and multiple later events") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")
    }

    it("should publish prerequisite event and multiple buffered events") {
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      val thirdOutput = outputTopic.readKeyValue()

      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "UPDATED")
    }
  }

}
