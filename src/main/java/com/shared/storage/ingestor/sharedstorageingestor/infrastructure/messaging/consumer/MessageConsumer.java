package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.messaging.consumer;

import java.io.Serializable;

public interface MessageConsumer {
    Serializable consume();
}
