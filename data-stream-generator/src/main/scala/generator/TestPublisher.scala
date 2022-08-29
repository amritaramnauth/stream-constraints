package generator

import java.util.Properties

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer

import scala.io.Source

class TestPublisher(kafkaProperties: Properties) extends Publisher {

  val kafkaProducerProps: Properties = {
    val props = new Properties()
    props.put("bootstrap.servers", kafkaProperties.getProperty("bootstrap.servers"))
    props.put("key.serializer", classOf[StringSerializer].getName)
    props.put("value.serializer", classOf[StringSerializer].getName)
    props
  }

  val producer = new KafkaProducer[String, String](kafkaProducerProps)

  val streamTimeAdvancement = "e1b24bca-b8c8-11eb-8529-0242ac130003 99999999 {\"date\": 1660837346, \"key\": 99999999, \"customerId\": \"yiiqptocra\", \"action\": \"CREATED\"}"

  override def publish(): Unit = {

    val source = Source.fromResource("data/order-events.txt")
    // val source = Source.fromResource("data/order-events-limited.txt")
    for (line <- source.getLines()) {

      val (eventId, orderId, event) = DataUtils.splitLine(line)
      val eventWithTime = event.replaceAll("\\b1660837346\\b", System.currentTimeMillis().toString)

      val record = new ProducerRecord[String, String]("order-events", orderId, eventWithTime)

      producer.send(record, new CompareProducerCallback)
      producer.flush()

      Thread.sleep(1)
    }

    source.close()
  }
}