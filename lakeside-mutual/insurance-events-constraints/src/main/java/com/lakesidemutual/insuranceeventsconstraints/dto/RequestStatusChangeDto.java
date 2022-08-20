package com.lakesidemutual.insuranceeventsconstraints.dto;

import java.util.Date;

/**
 * RequestStatusChangeDto is a data transfer object (DTO) that represents a status change of an insurance quote request.
 */
public class RequestStatusChangeDto {
	private Date date;
	private String status;

	public RequestStatusChangeDto() {
	}

	public RequestStatusChangeDto(Date date, String status) {
		this.date = date;
		this.status = status;
	}

	public Date getDate() {
		return date;
	}

	public void setDate(Date date) {
		this.date = date;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
