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

  val streamTimeAdvancement = "e1b24bca-b8c8-11eb-8529-0242ac130003 99999999 {\"date\": 1621422717, \"insuranceQuoteRequestId\": 99999999, \"policyId\": \"yiiqptocra\", \"$type\": \"PolicyCreatedEvent\"}"

  override def publish(): Unit = {

    val source = Source.fromResource("data/insurance-quote-and-decision-events.txt")
    for (line <- source.getLines()) {

      val (_, insuranceQuoteId, event) = DataUtils.splitLine(line)

      val eventWithTime = event.replaceAll("\\b1621422717\\b", System.currentTimeMillis().toString)

      val record = 
        
      if (event.contains("InsuranceQuoteRequestEvent")) {
       new ProducerRecord[String, String]("insurance-quote-request-events", insuranceQuoteId, eventWithTime)
      }
      //   if (event.contains("PolicyCreatedEvent")) {
      //   new ProducerRecord[String, String]("policy-created-events", insuranceQuoteId, eventWithTime)

      // if (event.contains("InsuranceQuoteResponseEvent")) {
      //   new ProducerRecord[String, String]("insurance-quote-response-events", insuranceQuoteId, eventWithTime)
      //  }
      // } 
      //  if (event.contains("CustomerDecisionEvent")) {
      //   new ProducerRecord[String, String]("customer-decision-events", insuranceQuoteId, eventWithTime)
      // }
       else {
        null
        // new ProducerRecord[String, String]("insurance-quote-expired-events", insuranceQuoteId, eventWithTime)
      }

      if (record != null) {
        producer.send(record, new CompareProducerCallback)
      }
      producer.flush()

      Thread.sleep(1)
    }

    Thread.sleep(2000)

    val (_, insuranceQuoteId, event) = DataUtils.splitLine(streamTimeAdvancement)
    val eventWithTime = event.replaceAll("\\b1621422717\\b", System.currentTimeMillis().toString)
    val record = new ProducerRecord[String, String]("policy-created-events", insuranceQuoteId, eventWithTime)

    producer.send(record, new CompareProducerCallback)
    producer.flush()

    source.close()
  }
}