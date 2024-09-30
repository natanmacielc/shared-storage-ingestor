package com.shared.storage.ingestor.sharedstorageingestor.app.usecase;

import java.io.Serializable;

public interface ProducerUseCase {
    void produce(Serializable data);
}
