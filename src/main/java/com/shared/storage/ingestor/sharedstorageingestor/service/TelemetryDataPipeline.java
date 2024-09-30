package com.shared.storage.ingestor.sharedstorageingestor.service;

import com.shared.storage.ingestor.sharedstorageingestor.domain.entity.LogEvent;

import java.io.Serializable;

public interface TelemetryDataPipeline<X extends LogEvent, Y extends Serializable> {
    void pipeline(X data);
}
