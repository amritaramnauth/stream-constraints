package com.lakesidemutual.insuranceeventsconstraints.dto;

import java.util.Date;

/**
 * InsuranceQuoteDto is a data transfer object (DTO) that represents an Insurance Quote
 * which has been submitted as a response to a specific Insurance Quote Request.
 */
public class InsuranceQuoteDto {
	private Date expirationDate;
	private MoneyAmountDto insurancePremium;
	private MoneyAmountDto policyLimit;

	public InsuranceQuoteDto() {
	}

	private InsuranceQuoteDto(Date expirationDate, MoneyAmountDto insurancePremium, MoneyAmountDto policyLimit) {
		this.expirationDate = expirationDate;
		this.insurancePremium = insurancePremium;
		this.policyLimit = policyLimit;
	}

	public Date getExpirationDate() {
		return expirationDate;
	}

	public void setExpirationDate(Date expirationDate) {
		this.expirationDate = expirationDate;
	}

	public MoneyAmountDto getInsurancePremium() {
		return insurancePremium;
	}

	public void setInsurancePremium(MoneyAmountDto insurancePremium) {
		this.insurancePremium = insurancePremium;
	}

	public MoneyAmountDto getPolicyLimit() {
		return policyLimit;
	}

	public void setPolicyLimit(MoneyAmountDto policyLimit) {
		this.policyLimit = policyLimit;
	}
}