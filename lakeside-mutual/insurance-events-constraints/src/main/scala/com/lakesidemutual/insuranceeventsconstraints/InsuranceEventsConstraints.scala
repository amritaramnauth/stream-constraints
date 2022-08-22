package com.lakesidemutual.insuranceeventsconstraints;

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraintBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, StreamsBuilder, StreamsConfig, Topology}

object InsuranceEventsConstraints extends App {

  val kafkaStreamsConfig: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "insurance-events-constraints-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092")
    properties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties
  }

  val insuranceQuoteEventSerde = Serdes.serdeFrom(InsuranceQuoteEventSerde.serializer(),
    InsuranceQuoteEventSerde.deserializer())

  val deduplicateConstraint = new DeduplicateConstraintBuilder[String, InsuranceQuoteRequestEvent]
    .deduplicate(((_, e) => e.`type` == "InsuranceQuoteRequestEvent", "insurance-quote-request"))
    .retentionPeriodMs(TimeUnit.MINUTES.toMillis(5))
    .valueComparator((event1, event2) => event1.getInsuranceQuoteRequestDto().getId().equals(event2.getInsuranceQuoteRequestDto().getId()))
 
  val constraint = new ConstraintBuilder[String, InsuranceQuoteRequestEvent, java.lang.Long]
    .deduplicate(deduplicateConstraint)
    .redirect("insurance-quote-events-redirect")
    .link((_, e) => e.getInsuranceQuoteRequestDto().getId())(Serdes.Long)
    .build(Serdes.String, insuranceQuoteEventSerde)

  val builder = new CStreamsBuilder()

  builder
    .stream("insurance-quote-events")(Consumed.`with`(Serdes.String, insuranceQuoteEventSerde))
    .constrain(constraint)
    .to("insurance-quote-request-events")(Produced.`with`(Serdes.String, insuranceQuoteEventSerde))
    // .to("insurance-quote-constrained")(Produced.`with`(Serdes.String, insuranceQuoteEventSerde))

  val topology: Topology = builder.build()

  val streams = new KafkaStreams(topology, kafkaStreamsConfig)

  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(5))
  }
}