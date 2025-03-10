package com.lakesidemutual.customerselfservice.interfaces;

import java.util.Date;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import com.lakesidemutual.customerselfservice.domain.MetricEvent;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteEntity;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteRequestAggregateRoot;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.InsuranceQuoteResponseEvent;
import com.lakesidemutual.customerselfservice.domain.insurancequoterequest.MoneyAmount;
import com.lakesidemutual.customerselfservice.infrastructure.EventMetricMessageProducer;
import com.lakesidemutual.customerselfservice.infrastructure.InsuranceQuoteRequestRepository;

/**
 * InsuranceQuoteResponseMessageConsumer is a Spring component that consumes InsuranceQuoteResponseEvents
 * as they arrive through the ActiveMQ message queue. It processes these events by updating the status
 * of the corresponding insurance quote requests.
 * */
@Component
public class InsuranceQuoteResponseMessageConsumer {
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	@Autowired
	private InsuranceQuoteRequestRepository insuranceQuoteRequestRepository;

	@Autowired
	private EventMetricMessageProducer eventMetricMessageProducer;

	@KafkaListener(topics = "${insuranceQuoteResponseEvent.topicName}",
			groupId = "${spring.kafka.consumer.group-id}",
			containerFactory = "insuranceQuoteResponseListenerFactory")
	public void receiveInsuranceQuoteResponse(final InsuranceQuoteResponseEvent insuranceQuoteResponseEvent) {
		logger.info("A new InsuranceQuoteResponseEvent has been received.");

		MetricEvent metricEvent = new MetricEvent(insuranceQuoteResponseEvent.getInsuranceQuoteRequestId().toString(), insuranceQuoteResponseEvent.getDate(), new Date(System.currentTimeMillis()), "InsuranceQuoteResponseEvent");
		eventMetricMessageProducer.sendEventMetricEvent(metricEvent);

		final Long id = insuranceQuoteResponseEvent.getInsuranceQuoteRequestId();
		final Optional<InsuranceQuoteRequestAggregateRoot> insuranceQuoteRequestOpt = insuranceQuoteRequestRepository.findById(id);

		if(!insuranceQuoteRequestOpt.isPresent()) {
			logger.error("Unable to process an insurance quote response event with an invalid insurance quote request id.");
			return;
		}

		final Date date = insuranceQuoteResponseEvent.getDate();
		final InsuranceQuoteRequestAggregateRoot insuranceQuoteRequest = insuranceQuoteRequestOpt.get();
		if(insuranceQuoteResponseEvent.isRequestAccepted()) {
			logger.info("The insurance quote request " + insuranceQuoteRequest.getId() + " has been accepted.");
			Date expirationDate = insuranceQuoteResponseEvent.getExpirationDate();
			MoneyAmount insurancePremium = insuranceQuoteResponseEvent.getInsurancePremium().toDomainObject();
			MoneyAmount policyLimit = insuranceQuoteResponseEvent.getPolicyLimit().toDomainObject();
			InsuranceQuoteEntity insuranceQuote = new InsuranceQuoteEntity(expirationDate, insurancePremium, policyLimit);
			insuranceQuoteRequest.acceptRequest(insuranceQuote, date);
		} else {
			logger.info("The insurance quote request " + insuranceQuoteRequest.getId() + " has been rejected.");
			insuranceQuoteRequest.rejectRequest(date);
		}

		insuranceQuoteRequestRepository.save(insuranceQuoteRequest);
	}
}
