package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@Data
@ConfigurationProperties(prefix = "arsenal.nat.telemetry")
public class TelemetryConfiguration {
    private String datasource;
}
