package com.shared.storage.ingestor.sharedstorageingestor.app.resource;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shared.storage.ingestor.sharedstorageingestor.domain.entity.LogEvent;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider.DataProvider;
import com.shared.storage.ingestor.sharedstorageingestor.service.impl.BatchTelemetryDataPipeline;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.FileSystemResource;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.CompletableFuture;

@RestController
@RequiredArgsConstructor
public class BatchResource {
    //    private final SharedStorageProvider sharedStorageProvider;
    private final BatchTelemetryDataPipeline batchTelemetryDataPipeline;

//    @PostMapping(value = "batch")
//    public CompletableFuture<ResponseEntity<String>> save(@RequestBody LogEvent logEvent) {
//        sharedStorageProvider.save(logEvent);
//        return CompletableFuture.supplyAsync(() -> ResponseEntity.ok("success"));
//    }

    @PostMapping(value = "batch2")
    public CompletableFuture<ResponseEntity<String>> save(@RequestBody LogEvent logEvent) {
        batchTelemetryDataPipeline.pipeline(logEvent);
        return CompletableFuture.supplyAsync(() -> ResponseEntity.ok("Log adicionado com sucesso."));
    }
}
