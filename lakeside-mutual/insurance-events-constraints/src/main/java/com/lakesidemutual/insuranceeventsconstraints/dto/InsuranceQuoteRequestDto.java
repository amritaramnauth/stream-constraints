package com.lakesidemutual.insuranceeventsconstraints.dto;

import java.util.Date;
import java.util.List;

/**
 * InsuranceQuoteRequestDto is a data transfer object (DTO) that represents a request
 * by a customer for a new insurance quote.
 */
public class InsuranceQuoteRequestDto {
	private Long id;
	private Date date;
	private List<RequestStatusChangeDto> statusHistory;
	private CustomerInfoDto customerInfo;
	private InsuranceOptionsDto insuranceOptions;
	private InsuranceQuoteDto insuranceQuote;
	private String policyId;

	public InsuranceQuoteRequestDto() {
	}

	public InsuranceQuoteRequestDto(Long id, Date date, List<RequestStatusChangeDto> statusHistory, CustomerInfoDto customerInfo, InsuranceOptionsDto insuranceOptions, InsuranceQuoteDto insuranceQuote, String policyId) {
		this.id = id;
		this.date = date;
		this.statusHistory = statusHistory;
		this.customerInfo = customerInfo;
		this.insuranceOptions = insuranceOptions;
		this.insuranceQuote = insuranceQuote;
		this.policyId = policyId;
	}

	public Long getId() {
		return id;
	}

	public void setId(Long id) {
		this.id = id;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public List<RequestStatusChangeDto> getStatusHistory() {
		return statusHistory;
	}

	public void setStatusHistory(List<RequestStatusChangeDto> statusHistory) {
		this.statusHistory = statusHistory;
	}

	public CustomerInfoDto getCustomerInfo() {
		return customerInfo;
	}

	public void setCustomerInfo(CustomerInfoDto customerInfo) {
		this.customerInfo = customerInfo;
	}

	public InsuranceOptionsDto getInsuranceOptions() {
		return insuranceOptions;
	}

	public void setInsuranceOptions(InsuranceOptionsDto insuranceOptions) {
		this.insuranceOptions = insuranceOptions;
	}

	public InsuranceQuoteDto getInsuranceQuote() {
		return insuranceQuote;
	}

	public void setInsuranceQuote(InsuranceQuoteDto insuranceQuote) {
		this.insuranceQuote = insuranceQuote;
	}

	public String getPolicyId() {
		return policyId;
	}

	public void setPolicyId(String policyId) {
		this.policyId = policyId;
	}
}
