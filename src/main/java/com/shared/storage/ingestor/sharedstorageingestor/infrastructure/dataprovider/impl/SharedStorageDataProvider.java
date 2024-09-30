package com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider.impl;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.shared.storage.ingestor.sharedstorageingestor.infrastructure.dataprovider.DataProvider;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.Serializable;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.shared.storage.ingestor.sharedstorageingestor.service.AbstractBatchTelemetryDataPipeline.BATCH_LIMIT;

@Component
@Slf4j
@RequiredArgsConstructor
public final class SharedStorageDataProvider implements DataProvider {

    private static final String LOG_DIR = "C:\\Users\\Natan\\dev\\projects\\shared-storage-ingestor\\log";
    private static final String BATCH_DIR = "C:\\Users\\Natan\\dev\\projects\\shared-storage-ingestor\\batch";
    private static final String READ_WRITE_MODE = "rw";
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final long EMPTY_FILE_LENGTH = 2L;
    private static final String LOG_FILE_PREFIX = "log-events-";
    private static final String BATCH_FILE_PREFIX = "batch-";
    private static final String JSON_FILE_TYPE = ".json";
    private static final byte[] EMPTY_ARRAY = "[]".getBytes();
    private RandomAccessFile randomAccessFile;
    private File lockedFile;
    @Getter
    private UUID uuid;

    @PostConstruct
    public final void initialize() {
        this.lockedFile = findOrCreateLockFile();
    }

    @Override
    public final void persist(Serializable data) {
        try {
            validateLockedFile();
            appendData(data);
            exportIfBatchSizeReached();
        } catch (IOException e) {
            log.error("Error while appending {} on {}", data, lockedFile.getAbsolutePath(), e);
        }
    }

    @Override
    public Integer count() {
        try {
            randomAccessFile.seek(0);
            final byte[] fileContent = readFileContent();
            return OBJECT_MAPPER.readTree(fileContent).size();
        } catch (IOException e) {
            throw new RuntimeException("Error counting entries in file", e);
        }
    }

    private void exportIfBatchSizeReached() throws IOException {
        if (count() >= BATCH_LIMIT) {
            if (batchFileExists()) {
                appendBatchFile();
            } else {
                createBatchFile();
            }
            clearLogEvents();
        }
    }

    private void appendBatchFile() throws IOException {
        final File file = new File(BATCH_DIR, BATCH_FILE_PREFIX.concat(uuid.toString()).concat(JSON_FILE_TYPE));
        try (final RandomAccessFile batchAccessFile = new RandomAccessFile(file, READ_WRITE_MODE)) {
            batchAccessFile.seek(batchAccessFile.length() - 1);
            randomAccessFile.seek(0);
            final String content = ",\n".concat(new String(readFileContent()).replace("[", ""));
            batchAccessFile.writeBytes(content);
        }
    }

    private boolean batchFileExists() {
        return Files.exists(Path.of(BATCH_DIR.concat("\\".concat(BATCH_FILE_PREFIX.concat(uuid.toString()).concat(JSON_FILE_TYPE)))));
    }

    private void validateLockedFile() {
        if (Objects.isNull(lockedFile)) {
            throw new IllegalStateException("No locked files available.");
        }
    }

    private <T extends Serializable> void appendData(T logData) throws IOException {
        final String json = OBJECT_MAPPER.writeValueAsString(logData);
        final long fileLength = randomAccessFile.length();
        randomAccessFile.seek(fileLength - 1);
        String dataToAppend = prepareDataForAppend(json, fileLength);
        randomAccessFile.writeBytes(dataToAppend);
        log.info("Data {} appended on file {}", logData, lockedFile.getAbsolutePath());
    }

    private String prepareDataForAppend(String logData, long fileLength) {
        if (fileLength == EMPTY_FILE_LENGTH) {
            return "\n" + logData + "\n]";
        } else {
            return ",\n" + logData + "\n]";
        }
    }

    private byte[] readFileContent() throws IOException {
        final byte[] fileContent = new byte[(int) randomAccessFile.length()];
        randomAccessFile.readFully(fileContent);
        return fileContent;
    }

    private File findOrCreateLockFile() {
        final File unlockedFile = getExistingLogFiles()
                .stream()
                .filter(this::isFileUnlocked)
                .findFirst()
                .orElseGet(this::createLogFile);
        return lockFile(unlockedFile);
    }

    private void setUniqueIdentifier(File file) {
        final int UUID_START = 11;
        final int UUID_END = 47;
        uuid = UUID.fromString(file.getName().substring(UUID_START, UUID_END));
    }

    private List<File> getExistingLogFiles() {
        final File dir = new File(LOG_DIR);
        if (!dir.exists() || !dir.isDirectory()) {
            try {
                Files.createDirectories(Path.of(LOG_DIR));
            } catch (IOException e) {
                throw new RuntimeException("Logs directory could not be created: ", e);
            }
        }
        return Arrays.stream(Objects.requireNonNull(dir.listFiles((d, name) -> name.startsWith(LOG_FILE_PREFIX) && name.endsWith(JSON_FILE_TYPE))))
                .collect(Collectors.toList());
    }

    private boolean isFileUnlocked(File file) {
        try (
                final FileOutputStream fileOutputStream = new FileOutputStream(file, true);
                final FileChannel channel = fileOutputStream.getChannel()
        ) {
            final FileLock lock = channel.tryLock();
            if (Objects.nonNull(lock)) {
                lock.release();
                return true;
            }
        } catch (IOException ignored) {
        }
        return false;
    }

    private File createLogFile() {
        final File file = new File(LOG_DIR, LOG_FILE_PREFIX.concat(UUID.randomUUID().toString()).concat(JSON_FILE_TYPE));
        try {
            Files.createDirectories(Path.of(LOG_DIR));
            if (file.createNewFile()) {
                addContent(file, EMPTY_ARRAY);
                return file;
            } else {
                throw new IOException("Could not create log file: " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void createBatchFile() throws IOException {
        final File file = new File(BATCH_DIR, BATCH_FILE_PREFIX.concat(uuid.toString()).concat(JSON_FILE_TYPE));
        randomAccessFile.seek(0);
        final byte[] content = readFileContent();
        Files.createDirectories(Path.of(BATCH_DIR));
        if (file.createNewFile()) {
            addContent(file, content);
        } else {
            throw new IOException("Could not create batch file: " + file.getAbsolutePath());
        }
    }

    private void clearLogEvents() throws IOException {
        randomAccessFile.setLength(0);
        randomAccessFile.write(EMPTY_ARRAY);
    }

    private void addContent(File file, byte[] content) throws IOException {
        try (final FileOutputStream fileOutputStream = new FileOutputStream(file)) {
            fileOutputStream.write(content);
        }
    }

    private File lockFile(File file) {
        try {
            setUniqueIdentifier(file);
            randomAccessFile = new RandomAccessFile(file, READ_WRITE_MODE);
            final FileChannel fileChannel = randomAccessFile.getChannel();
            final FileLock lock = fileChannel.lock();
            if (Objects.nonNull(lock)) {
                log.info("Locked file {}", file.getAbsolutePath());
                return file;
            } else {
                throw new IOException("Could not lock file: " + file.getAbsolutePath());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
