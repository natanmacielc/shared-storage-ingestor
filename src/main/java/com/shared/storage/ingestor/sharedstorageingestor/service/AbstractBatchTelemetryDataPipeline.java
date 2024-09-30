package com.shared.storage.ingestor.sharedstorageingestor.service;

import com.shared.storage.ingestor.sharedstorageingestor.domain.entity.LogEvent;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.config.TelemetryConfiguration;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider.DataProvider;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.JobExecutionException;
import org.springframework.batch.core.JobParameters;
import org.springframework.batch.core.launch.JobLauncher;
import org.springframework.context.ApplicationContext;

import java.io.Serializable;

public abstract class AbstractBatchTelemetryDataPipeline implements TelemetryDataPipeline<LogEvent, Serializable> {
    public static final int BATCH_LIMIT = 50;
    private final DataProvider dataProvider;
    private final JobLauncher jobLauncher;
    private final Job job;

    protected AbstractBatchTelemetryDataPipeline(ApplicationContext applicationContext, TelemetryConfiguration telemetryConfiguration, JobLauncher jobLauncher, Job job) {
        this.jobLauncher = jobLauncher;
        this.job = job;
        this.dataProvider = applicationContext.getBean(DataProvider.class, telemetryConfiguration.getDatasource().concat(DataProvider.class.getSimpleName()));
    }

    @Override
    public void pipeline(LogEvent data) {
        persist(data);
        deliver();
    }

    protected void persist(LogEvent data) {
        dataProvider.persist(data);
    }

    protected void deliver() {
        if (dataProvider.count() == 0) {
            try {
                jobLauncher.run(job, new JobParameters());
            } catch (JobExecutionException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
