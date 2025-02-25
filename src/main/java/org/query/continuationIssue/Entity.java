package org.query.continuationIssue;

import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jsr310.ser.LocalDateTimeSerializer;

import java.time.LocalDateTime;
import java.util.Date;

public class Entity {

    private String id;

    private String expectedProcessTime;

    private String documentType;

    private String status;

    public Entity() {
    }

    public String getId() {
        return id;
    }

    public Entity setId(String id) {
        this.id = id;
        return this;
    }

    public String getStatus() {
        return status;
    }

    public Entity setStatus(String status) {
        this.status = status;
        return this;
    }

    public String getDocumentType() {
        return documentType;
    }

    public Entity setDocumentType(String documentType) {
        this.documentType = documentType;
        return this;
    }
    public String getExpectedProcessTime() {
        return expectedProcessTime;
    }

    public Entity setExpectedProcessTime(String expectedProcessTime) {
        this.expectedProcessTime = expectedProcessTime;
        return this;
    }
}
