package com.lakesidemutual.insuranceeventsconstraints

import com.fasterxml.jackson.databind.{ObjectMapper, DeserializationFeature}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.kafka.common.serialization.{Deserializer, Serde, Serializer}

object InsuranceQuoteEventSerde extends Serde[InsuranceQuoteRequestEvent]  {

  val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)

  override def serializer(): Serializer[InsuranceQuoteRequestEvent] = (topic: String, data: InsuranceQuoteRequestEvent) => {
    mapper.writeValueAsBytes(data)
  }

  override def deserializer(): Deserializer[InsuranceQuoteRequestEvent] = (topic: String, data: Array[Byte]) => {
    mapper.readValue(data, classOf[InsuranceQuoteRequestEvent])
  }
}