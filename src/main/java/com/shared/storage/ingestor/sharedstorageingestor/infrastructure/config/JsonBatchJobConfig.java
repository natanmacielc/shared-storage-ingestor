package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shared.storage.ingestor.sharedstorageingestor.domain.entity.LogEvent;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider.impl.SharedStorageDataProvider;
import lombok.RequiredArgsConstructor;
import org.checkerframework.checker.units.qual.A;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.EnableBatchProcessing;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.core.step.tasklet.Tasklet;
import org.springframework.batch.integration.async.AsyncItemProcessor;
import org.springframework.batch.item.Chunk;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.json.JacksonJsonObjectMarshaller;
import org.springframework.batch.item.json.JacksonJsonObjectReader;
import org.springframework.batch.item.json.JsonFileItemWriter;
import org.springframework.batch.item.json.JsonItemReader;
import org.springframework.batch.item.json.builder.JsonFileItemWriterBuilder;
import org.springframework.batch.item.json.builder.JsonItemReaderBuilder;
import org.springframework.batch.item.support.CompositeItemProcessor;
import org.springframework.batch.repeat.RepeatOperations;
import org.springframework.batch.repeat.RepeatStatus;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;

import javax.sql.DataSource;
import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

@Configuration
@ConditionalOnBean(value = SharedStorageDataProvider.class)
@EnableBatchProcessing(dataSourceRef = "batchDataSource", transactionManagerRef = "batchTransactionManager")
@RequiredArgsConstructor
public class JsonBatchJobConfig {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger log = LoggerFactory.getLogger(JsonBatchJobConfig.class);
    private final SharedStorageDataProvider sharedStorageDataProvider;

    @Bean
    public DataSource batchDataSource() {
        return new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2)
                .addScript("/org/springframework/batch/core/schema-h2.sql")
                .generateUniqueName(true).build();
    }

    @Bean
    public JdbcTransactionManager batchTransactionManager(DataSource dataSource) {
        return new JdbcTransactionManager(dataSource);
    }

    @Bean
    public JsonItemReader<LogEvent> jsonItemReader() {
        return new JsonItemReaderBuilder<LogEvent>()
                .jsonObjectReader(new JacksonJsonObjectReader<>(LogEvent.class))
                .resource(new FileSystemResource("C:\\Users\\Natan\\dev\\projects\\shared-storage-ingestor\\batch\\batch-" + sharedStorageDataProvider.getUuid() + ".json"))
                .name("logEventJsonItemReader")
                .build();
    }

    @Bean
    public AsyncItemProcessor<LogEvent, LogEvent> asyncJsonItemProcessor() {
        final AsyncItemProcessor<LogEvent, LogEvent> asyncItemProcessor = new AsyncItemProcessor<>();
        final SimpleAsyncTaskExecutor executor = new SimpleAsyncTaskExecutor("spring_batch_async");
        executor.setConcurrencyLimit(10);
        asyncItemProcessor.setTaskExecutor(executor);
        asyncItemProcessor.setDelegate(item -> {
            try {
                log.info(new ObjectMapper().writeValueAsString(item));
                return null;
            } catch (IOException e) {
                return item;
            }
        });
        return asyncItemProcessor;
    }

    @Bean
    public ItemProcessor<Future<LogEvent>, LogEvent> itemProcessor() {
        return item -> {
            final LogEvent logEvent = item.get();
            if (Objects.nonNull(logEvent)) {
                log.info(OBJECT_MAPPER.writeValueAsString(logEvent));
                return logEvent;
            } else {
                return null;
            }
        };
    }

    @Bean
    public ItemProcessor<LogEvent, LogEvent> compositeItemProcessor() {
        CompositeItemProcessor<LogEvent, LogEvent> compositeItemProcessor = new CompositeItemProcessor<>();
        compositeItemProcessor.setDelegates(Arrays.asList(asyncJsonItemProcessor(), itemProcessor()));
        return compositeItemProcessor;
    }

    @Bean
    public JsonFileItemWriter<LogEvent> jsonItemWriter() {
        return new JsonFileItemWriterBuilder<LogEvent>()
                .name("jsonItemWriter")
                .resource(new FileSystemResource("C:\\Users\\Natan\\dev\\projects\\shared-storage-ingestor\\batch\\test-batch-" + sharedStorageDataProvider.getUuid() + ".json"))
                .jsonObjectMarshaller(new JacksonJsonObjectMarshaller<>())
                .build();
    }

    @Bean
    public Step jsonStep(
            JobRepository jobRepository,
            PlatformTransactionManager transactionManager
    ) {
        return new StepBuilder("sampleStep", jobRepository)
                .<LogEvent, LogEvent>chunk(5, transactionManager)
                .reader(jsonItemReader())
                .processor(compositeItemProcessor())
                .writer(jsonItemWriter())
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Tasklet fileDeletingTasklet() {
        return (contribution, chunkContext) -> {
            final File file = new FileSystemResource("C:\\Users\\Natan\\dev\\projects\\shared-storage-ingestor\\batch\\batch-" + sharedStorageDataProvider.getUuid() + ".json").getFile();
            if (file.exists()) {
                boolean deleted = file.delete();
                if (deleted) {
                    log.info("Batch file deleted {}", file.getAbsolutePath());
                } else {
                    log.info("Log file {} could not be deleted", file.getAbsolutePath());
                }
            }
            return RepeatStatus.FINISHED;
        };
    }

    @Bean
    public Step deleteFileStep(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new StepBuilder("deleteFileStep", jobRepository)
                .tasklet(fileDeletingTasklet(), transactionManager)
                .allowStartIfComplete(true)
                .build();
    }

    @Bean
    public Job jsonProcessingJob(JobRepository jobRepository, PlatformTransactionManager transactionManager) {
        return new JobBuilder("logEventJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(jsonStep(jobRepository, transactionManager))
                .next(deleteFileStep(jobRepository, transactionManager))
                .build();
    }
}
