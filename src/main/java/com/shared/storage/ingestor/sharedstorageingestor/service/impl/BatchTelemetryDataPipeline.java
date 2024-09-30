package com.shared.storage.ingestor.sharedstorageingestor.service.impl;

import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.config.TelemetryConfiguration;
import com.shared.storage.ingestor.sharedstorageingestor.service.AbstractBatchTelemetryDataPipeline;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

@Service
public class BatchTelemetryDataPipeline extends AbstractBatchTelemetryDataPipeline {
    @Autowired
    protected BatchTelemetryDataPipeline(ApplicationContext applicationContext, TelemetryConfiguration telemetryConfiguration, JobLauncher jobLauncher, Job job) {
        super(applicationContext, telemetryConfiguration, jobLauncher, job);
    }

}
