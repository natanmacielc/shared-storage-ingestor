package com.shared.storage.ingestor.sharedstorageingestor.domain.entity;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serial;
import java.io.Serializable;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class LogEvent implements Serializable {
    @Serial
    private static final long serialVersionUID = 1L;
    private String message;
    private String timestamp;
    private String ipAddress;
    private String journey;
    private String subJourney;
    private String type;
    private String application;
    private String hostname;
    private String product;
    private String transaction;
    private String returnCode;
    private String acronym;
    private String span;
    private String correlationId;
    private String channel;
}
