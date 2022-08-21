package com.lakesidemutual.policymanagement.domain;

import java.util.Date;

import org.microserviceapipatterns.domaindrivendesign.DomainEvent;

public class MetricEvent implements DomainEvent {
    private String eventId;
    private Date producedAt;
    private Date consumedAt;
    private String eventType;

    
    public MetricEvent() {}

    public MetricEvent(String eventId, Date producedAt, Date consumedAt, String eventType) {
        this.eventId = eventId;
        this.producedAt = producedAt;
        this.consumedAt = consumedAt;
        this.eventType = eventType;
    }

    public String getEventId() {
        return eventId;
    }

    public void setEventId(String eventId) {
        this.eventId = eventId;
    }

    public Date getProducedAt() {
        return producedAt;
    }

    public void setProducedAt(Date producedAt) {
        this.producedAt = producedAt;
    }
    
    public Date getConsumedAt() {
        return consumedAt;
    }

    public void setConsumedAt(Date consumedAt) {
        this.consumedAt = consumedAt;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    
}
