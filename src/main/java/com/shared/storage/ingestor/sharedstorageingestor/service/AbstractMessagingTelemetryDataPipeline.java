package com.shared.storage.ingestor.sharedstorageingestor.service;

import com.shared.storage.ingestor.sharedstorageingestor.domain.entity.LogEvent;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.config.TelemetryConfiguration;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.messaging.producer.MessageProducer;
import org.springframework.context.ApplicationContext;

import java.io.Serializable;

public abstract class AbstractMessagingTelemetryDataPipeline implements TelemetryDataPipeline<LogEvent, Serializable> {
    private final MessageProducer producer;

    protected AbstractMessagingTelemetryDataPipeline(ApplicationContext applicationContext, TelemetryConfiguration telemetryConfiguration) {
        this.producer = applicationContext.getBean(MessageProducer.class, telemetryConfiguration.getDatasource().concat(MessageProducer.class.getSimpleName()));
    }

    @Override
    public void pipeline(LogEvent logEvent) {
        persist(logEvent);
    }

    protected void persist(LogEvent data) {
        producer.produce(data);
    }
}
