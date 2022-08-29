package generator

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

class ExtendedPublisher(kafkaProperties: Properties) extends Publisher {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  override def publish(): Unit = {

    val source = Source.fromResource("data/extended-events-1000.txt")
    // val source = Source.fromResource("data/extended-events-1500.txt")
    // val source = Source.fromResource("data/extended-events-2000.txt")
    // val source = Source.fromResource("data/extended-events-2500.txt")
    for (line <- source.getLines()) {

      val (eventId, insuranceQuoteId, event) = DataUtils.splitLine(line)
      val eventWithProducerTime = event.replaceAll("\\b1661732324\\b", System.currentTimeMillis().toString)
      
      val record = if (event.contains("InsuranceQuoteRequestEvent")) {
        new ProducerRecord[String, String]("insurance-quote-request-events", insuranceQuoteId, eventWithProducerTime)
        // new ProducerRecord[String, String]("insurance-quote-events", insuranceQuoteId, eventWithProducerTime)
      } else {
        new ProducerRecord[String, String]("policy-events", insuranceQuoteId, eventWithProducerTime)
        // new ProducerRecord[String, String]("policy-events-constrained", insuranceQuoteId, eventWithProducerTime)
      }

      producer.send(record, new CompareProducerCallback)
      producer.flush()

      Thread.sleep(1)
    }

    source.close()
  }
}