package com.lakesidemutual.customerselfservice.infrastructure;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;
import org.microserviceapipatterns.domaindrivendesign.InfrastructureService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import com.lakesidemutual.customerselfservice.domain.MetricEvent;

/**
 * EventMetricsMessageProducer sends the produced and consumed metrics to the Kafka broker.
 * */
@Component
public class EventMetricMessageProducer implements InfrastructureService {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Value("${eventMetricEvent.topicName}")
	private String eventMetricTopic;

	@Autowired
	private KafkaTemplate<String, DomainEvent> kafkaTemplate;

	public void sendEventMetricEvent(MetricEvent event) {
		try {
			kafkaTemplate.send(eventMetricTopic, event.getEventId().toString(), event);
			logger.info("Successfully sent an EventMetric event.");
		} catch(Exception exception) {
			logger.error("Failed to send an EventMetric event.", exception);
		}
	}

}