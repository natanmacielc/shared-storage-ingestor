package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider;

import java.io.Serializable;

public interface DataProvider {
    void persist(Serializable data);
    Integer count();
}
