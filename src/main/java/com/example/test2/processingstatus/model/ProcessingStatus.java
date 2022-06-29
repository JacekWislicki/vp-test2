package com.example.test2.processingstatus.model;

import java.io.Serializable;

import com.example.test2.processingstatus.utils.IdGenerator;
import com.example.test2.processingstatus.utils.ObjectConverter;

import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
public class ProcessingStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        FATAL,
        ERROR,
        WARNING,
        RECYCLING,
        NOT_PROCESSED,
        OK,
        INFO
    }

    @SuppressWarnings("java:S116")
    private long _timestamp = System.currentTimeMillis();
    @SuppressWarnings("java:S116")
    private String _makoId = IdGenerator.generate();
    private Stage stage;
    private Status status;
    private String vin;
    private String urn;
    private String message;
    private String event;
    private Integer code;
    private String details;

    public ProcessingStatus(String vin, String urn, Stage stage, Status status, String message, Object event) {
        this(vin, urn, stage, status, message, event, null);
    }

    public ProcessingStatus(String vin, String urn, Stage stage, Status status, String message, Object event, Problem problem) {
        this.vin = vin;
        this.urn = urn;
        this.stage = stage;
        this.status = status;
        this.message = message;
        this.event = ObjectConverter.toString(event);
        if (problem != null) {
            this.code = problem.getCode();
            this.details = problem.getDetails();
        }
    }
}