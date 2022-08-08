package com.github.annterina.stream_constraints.example.deduplicate

import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import com.github.annterina.stream_constraints.constraints.limit.LimitConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec

class OrderApplicationLimitSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _
  private var redirectTopic: TestOutputTopic[String, OrderEvent] = _

  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val limitOrderCreatedConstraint = new LimitConstraintBuilder[String, OrderEvent]
    .limit((_, e) => e.action == "UPDATED", "order-updated")
    .numberToLimit(3) 

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .limitConstraint(limitOrderCreatedConstraint)
      .redirect("limit-orders-redirect")
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

    redirectTopic = testDriver.createOutputTopic(
      "limit-orders-redirect",
      Serdes.String.deserializer(),
      OrderEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Application with limit constraint") {

    it("should redirect exceeded events") {
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("456", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))
      inputTopic.pipeInput("123", OrderEvent(1, "UPDATED"))


      val firstOutput = outputTopic.readKeyValue()
      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "UPDATED")

      val secondOutput = outputTopic.readKeyValue()
      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "UPDATED")

      
      val thirdOutput = outputTopic.readKeyValue()
      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "UPDATED")

      val fourthOutput = outputTopic.readKeyValue()
      assert(fourthOutput.key == "123")
      assert(fourthOutput.value.key == 1)
      assert(fourthOutput.value.action == "UPDATED")

      assert(outputTopic.getQueueSize() === 4)
      
      val firstRedirectOutput = redirectTopic.readKeyValue()
      assert(firstRedirectOutput.key == "123")
      assert(firstRedirectOutput.value.key == 1)
      assert(firstRedirectOutput.value.action == "UPDATED")

      val secondRedirectOutput = redirectTopic.readKeyValue()
      assert(secondRedirectOutput.key == "123")
      assert(secondRedirectOutput.value.key == 1)
      assert(secondRedirectOutput.value.action == "UPDATED")

      assert(!redirectTopic.isEmpty)
    }

  }

}
