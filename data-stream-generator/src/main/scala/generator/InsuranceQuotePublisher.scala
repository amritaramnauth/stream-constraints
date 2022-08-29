package generator

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

class InsuranceQuotePublisher(kafkaProperties: Properties) extends Publisher {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  // val streamTimeAdvancement = "e1b24bca-b8c8-11eb-8529-0242ac130003 99999999 {\"date\": 1661041150, \"insuranceQuoteRequestId\": 99999999, \"policyId\": \"yiiqptocra\", \"$type\": \"PolicyCreatedEvent\"}"

  override def publish(): Unit = {

    val source = Source.fromResource("data/quote-events.txt")
    for (line <- source.getLines()) {

      val (_, insuranceQuoteId, event) = DataUtils.splitLine(line)

      val eventWithProducedTime = event.replaceAll("\\b1661041150\\b", System.currentTimeMillis().toString)

      val record = if (event.contains("InsuranceQuoteRequestEvent")) {
        new ProducerRecord[String, String]("insurance-quote-request-events", insuranceQuoteId, eventWithProducedTime)
      } else if (event.contains("InsuranceQuoteResponseEvent")) {
        new ProducerRecord[String, String]("insurance-quote-response-events", insuranceQuoteId, eventWithProducedTime)
      } else  {
        new ProducerRecord[String, String]("customer-decision-events", insuranceQuoteId, eventWithProducedTime)
      }
      //    else {
      //   new ProducerRecord[String, String]("policy-created-events", insuranceQuoteId, eventWithTime)
      // }

      producer.send(record, new CompareProducerCallback)
      producer.flush()

      Thread.sleep(1)
    }

    source.close()
  }
}