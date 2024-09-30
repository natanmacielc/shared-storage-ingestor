package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.messaging.producer;

import java.io.Serializable;

public interface MessageProducer {
    void produce(Serializable data);
}
