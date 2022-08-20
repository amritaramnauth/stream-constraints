package com.lakesidemutual.insuranceeventsconstraints;

import com.lakesidemutual.insuranceeventsconstraints.dto.InsuranceQuoteRequestDto;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonTypeName;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.util.Date;


/**
 * InsuranceQuoteRequestEvent is a domain event that is sent to the Policy Management backend
 * every time a customer submits an insurance quote.
 * */
@JsonIgnoreProperties({ "$type" })
public class InsuranceQuoteRequestEvent implements DomainEvent {
	private Date date;
    private InsuranceQuoteRequestDto insuranceQuoteRequestDto;

	@JsonProperty("type")
	public final String type = "InsuranceQuoteRequestEvent";

	public InsuranceQuoteRequestEvent() {
	}

	public InsuranceQuoteRequestEvent(Date date, InsuranceQuoteRequestDto insuranceQuoteRequestDto) {
		this.date = date;
        this.insuranceQuoteRequestDto = insuranceQuoteRequestDto;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

    public InsuranceQuoteRequestDto getInsuranceQuoteRequestDto() {
        return insuranceQuoteRequestDto;
    }


    public void setInsuranceQuoteRequestDto(InsuranceQuoteRequestDto insuranceQuoteRequestDto) {
        this.insuranceQuoteRequestDto = insuranceQuoteRequestDto;
    }

}