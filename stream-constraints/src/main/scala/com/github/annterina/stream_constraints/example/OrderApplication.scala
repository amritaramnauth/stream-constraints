package com.github.annterina.stream_constraints.example

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.limit.LimitConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraintBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.{KafkaStreams, StreamsConfig, Topology}
import org.apache.kafka.streams.kstream.{Consumed, Produced}

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit
import org.slf4j.{Logger, LoggerFactory}
import com.github.annterina.stream_constraints.constraints.Prerequisite

object OrderApplication extends App {

  private lazy val logger: Logger = LoggerFactory.getLogger(this.getClass)

  val kafkaStreamsConfig: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "order-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "Amrita:9092")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties
  }

  val orderEventSerde = Serdes.serdeFrom(OrderEventSerde.serializer(), OrderEventSerde.deserializer())

  val deduplicateOrderCreatedConstraint = new DeduplicateConstraintBuilder[String, OrderEvent]
    .deduplicate((_, e) => e.action == "CREATED", "order-created")
    .retentionPeriodMs(TimeUnit.MINUTES.toMillis(1))  

  val limit = new LimitConstraintBuilder[String, OrderEvent]
    .limit((_, e) => e.action == "UPDATED", "order-updated")
    .numberToLimit(3)

  val constraint = new ConstraintBuilder[String, OrderEvent, Integer]
      // .deduplicate(deduplicateOrderCreatedConstraint)
      // .limitConstraint(limit)
       .prerequisite(((_, e) => e.action == "CREATED", "order-created"), 
      ((_, e) => e.action == "UPDATED", "order-updated"), TimeUnit.SECONDS.toMillis(10))
      .prerequisite(((_, e) => e.action == "CREATED", "order-created"), 
      ((_, e) => e.action == "DELETED", "order-deleted"))
      .redirect("orders-redirect")
      .link((_, e) => e.key)(Serdes.Integer)
      .build(Serdes.String, orderEventSerde)

  val builder = new CStreamsBuilder()

  builder
    .stream("orders")(Consumed.`with`(Serdes.String, orderEventSerde))
    .selectKey((_, value) => value.key.toString)
    .constrain(constraint)
    .to("orders-output-topic")(Produced.`with`(Serdes.String, orderEventSerde))

  val topology: Topology = builder.build()

  logger.info(topology.describe().toString)
  val streams = new KafkaStreams(topology, kafkaStreamsConfig)

  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(5))
  }

}
