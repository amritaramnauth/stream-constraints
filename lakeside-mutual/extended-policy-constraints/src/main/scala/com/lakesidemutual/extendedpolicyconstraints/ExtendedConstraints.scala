package com.lakesidemutual.extendedpolicyconstraints

import java.time.Duration
import java.util.Properties
import java.util.concurrent.TimeUnit

import com.github.annterina.stream_constraints.CStreamsBuilder
import com.github.annterina.stream_constraints.constraints.ConstraintBuilder
import com.github.annterina.stream_constraints.constraints.window.WindowConstraintBuilder
import com.github.annterina.stream_constraints.constraints.deduplicate.DeduplicateConstraintBuilder
import com.github.annterina.stream_constraints.constraints.limit.LimitConstraintBuilder
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.scala.kstream.{Consumed, Produced}
import org.apache.kafka.streams.{KafkaStreams, KeyValue, StreamsConfig, Topology}

object ExtendedPolicyConstraints extends App {

  val kafkaStreamsConfig: Properties = {
    val properties = new Properties()
    properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "extended-constraints-application")
    properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "Amrita:9092")
    properties.put(StreamsConfig.METRICS_RECORDING_LEVEL_CONFIG, "DEBUG")
    properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    properties
  }

  val policyEventSerde = Serdes.serdeFrom(PolicyEventSerde.serializer(), PolicyEventSerde.deserializer())
  
  val deduplicateConstraint = new DeduplicateConstraintBuilder[String, PolicyDomainEvent]
    .deduplicate(((_, e) => e.`type` == "DeletePolicyEvent", "policy-deleted"))
    .retentionPeriodMs(TimeUnit.MINUTES.toMillis(5))
    .valueComparator((event1, event2) => event1.policyId().equals(event2.policyId()))

  val limit = new LimitConstraintBuilder[String, PolicyDomainEvent]
    .limit((_, e) => e.`type` == "UpdatePolicyEvent", "policy-updated")
    .numberToLimit(2)


  val constraint = new ConstraintBuilder[String, PolicyDomainEvent, String]
    .deduplicate(deduplicateConstraint)
    .limitConstraint(limit)
    .prerequisite(((_, e) => e.`type` == "UpdatePolicyEvent", "policy-updated"), 
    ((_, e) => e.`type` == "DeletePolicyEvent", "policy-deleted"), TimeUnit.MINUTES.toMillis(1))
    .redirect("policy-events-redirect")
    .link((_, e) => e.policyId())(Serdes.String)
    .build(Serdes.String, policyEventSerde)

  val builder = new CStreamsBuilder()

  builder
    .stream("policy-events")(Consumed.`with`(Serdes.String, policyEventSerde))
    .constrain(constraint)
    .to("policy-events-constrained")(Produced.`with`(Serdes.String, policyEventSerde))

  val topology: Topology = builder.build()

  val streams = new KafkaStreams(topology, kafkaStreamsConfig)

  streams.cleanUp()
  streams.start()

  sys.ShutdownHookThread {
    streams.close(Duration.ofSeconds(5))
  }
}