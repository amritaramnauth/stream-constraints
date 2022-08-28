package com.lakesidemutual.customerselfservice.interfaces;

import java.util.Date;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.lakesidemutual.customerselfservice.domain.MetricEvent;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteRequestAggregateRoot;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.PolicyCreatedEvent;
import com.lakesidemutual.customerselfservice.infrastructure.EventMetricMessageProducer;
import com.lakesidemutual.customerselfservice.infrastructure.InsuranceQuoteRequestRepository;

/**
 * PolicyCreatedMessageConsumer is a Spring component that consumes PolicyCreatedEvents
 * as they arrive through the Kafka message topic. It processes these events by updating
 * the status of the corresponding insurance quote requests.
 * */
@Component
public class PolicyCreatedMessageConsumer {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private InsuranceQuoteRequestRepository insuranceQuoteRequestRepository;

	@Autowired
	private EventMetricMessageProducer eventMetricMessageProducer;

	@KafkaListener(topics = "${policyCreatedEvent.topicName}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "policyCreatedListenerFactory")
	public void receivePolicyCreatedEvent(final PolicyCreatedEvent policyCreatedEvent) {
		logger.info("A new PolicyCreatedEvent has been received.");
		
		final Long id = policyCreatedEvent.getInsuranceQuoteRequestId();
		final Optional<InsuranceQuoteRequestAggregateRoot> insuranceQuoteRequestOpt = insuranceQuoteRequestRepository.findById(id);

		if (!insuranceQuoteRequestOpt.isPresent()) {
			logger.error("Unable to process a policy created event with an invalid insurance quote request id.");
			return;
		}

		// publish event metric event
		MetricEvent metricEvent = new MetricEvent(policyCreatedEvent.getPolicyId().toString(), policyCreatedEvent.getDate(), new Date(System.currentTimeMillis()), "PolicyCreatedEvent");
		eventMetricMessageProducer.sendEventMetricEvent(metricEvent);

		final InsuranceQuoteRequestAggregateRoot insuranceQuoteRequest = insuranceQuoteRequestOpt.get();
		insuranceQuoteRequest.finalizeQuote(policyCreatedEvent.getPolicyId(), policyCreatedEvent.getDate());
		logger.info("A policy was created for the insurance quote request " + insuranceQuoteRequest.getId() + ".");
		insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
	}
}