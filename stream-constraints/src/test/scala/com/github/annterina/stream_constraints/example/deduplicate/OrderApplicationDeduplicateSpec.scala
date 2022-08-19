package com.github.annterina.stream_constraints.example.deduplicate

import java.util.Properties

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.example.{OrderEvent, OrderEventSerde}
import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraintBuilder
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{StreamsConfig, TestInputTopic, TestOutputTopic, TopologyTestDriver}
import org.scalatest.BeforeAndAfterEach
import org.scalatest.funspec.AnyFunSpec
import java.util.concurrent.TimeUnit
import java.util.Date


class OrderApplicationDeduplicateSpec extends AnyFunSpec with BeforeAndAfterEach {

  private var testDriver: TopologyTestDriver = _
  private var inputTopic: TestInputTopic[String, OrderEvent] = _
  private var outputTopic: TestOutputTopic[String, OrderEvent] = _
  private var redirectTopic: TestOutputTopic[String, OrderEvent] = _
  
  override def beforeEach(): Unit = {
    val config = new Properties()
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application-test")
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234")

    val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

    val deduplicateOrderCreatedConstraint = new DeduplicateConstraintBuilder[String, OrderEvent]
    .deduplicate((_, e) => e.action == "CREATED", "order-created")
    .retentionPeriodMs(TimeUnit.MINUTES.toMillis(1))
    .valueComparator((event1, event2) => event1.action == event2.action) 

    val constraints = new ConstraintBuilder[String, OrderEvent, Integer]
      .deduplicate(deduplicateOrderCreatedConstraint)
      .redirect("deduplicate-orders-redirect")
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
      "deduplicate-orders-redirect",
      Serdes.String.deserializer(),
      OrderEventSerde.deserializer()
    )
  }

  override def afterEach(): Unit = {
    testDriver.getAllStateStores.clear()
    testDriver.close()
  }

  describe("Order Application with deduplicate constraint") {

    it("should publish all unique events") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))

      val firstOutput = outputTopic.readKeyValue()

      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()

      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CREATED")

      
      val thirdOutput = outputTopic.readKeyValue()
      
      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CREATED")

      assert(redirectTopic.isEmpty)
      assert(outputTopic.getQueueSize() === 3)
    }


    it("should redirect deduplicate events") {
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("456", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("789", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))
      inputTopic.pipeInput("123", OrderEvent(1, new Date("1660931536"), "customer1", "CREATED"))


      val firstOutput = outputTopic.readKeyValue()
      assert(firstOutput.key == "123")
      assert(firstOutput.value.key == 1)
      assert(firstOutput.value.action == "CREATED")

      val secondOutput = outputTopic.readKeyValue()
      assert(secondOutput.key == "456")
      assert(secondOutput.value.key == 1)
      assert(secondOutput.value.action == "CREATED")

      
      val thirdOutput = outputTopic.readKeyValue()
      assert(thirdOutput.key == "789")
      assert(thirdOutput.value.key == 1)
      assert(thirdOutput.value.action == "CREATED")
      
      assert(outputTopic.getQueueSize() === 3)

      
      val firstDeduplicateOutput = redirectTopic.readKeyValue()
      assert(firstDeduplicateOutput.key == "456")
      assert(firstDeduplicateOutput.value.key == 1)
      assert(firstDeduplicateOutput.value.action == "CREATED")
      
      val secondDeduplicateOutput = redirectTopic.readKeyValue()
      assert(secondDeduplicateOutput.key == "123")
      assert(secondDeduplicateOutput.value.key == 1)
      assert(secondDeduplicateOutput.value.action == "CREATED")
      
      assert(!redirectTopic.isEmpty)
    }

  }

}
