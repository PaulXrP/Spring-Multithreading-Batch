package com.dev.pranay.Multithreaded.Batched.Processing.utilities;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;

@Slf4j
public class DeadLetterHandler {

    private final String deadLetterFilePath;
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public DeadLetterHandler(String baseFileName) {
        this.deadLetterFilePath = baseFileName + "_" + LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss")) + ".dql";

        try {
            Path dlqPath = Paths.get(this.deadLetterFilePath);
            Path parentDir = dlqPath.getParent();
            if(parentDir != null) { // Only create directories if a parent path exists
                Files.createDirectories(parentDir);
            }
//            Files.createDirectories(Paths.get(this.deadLetterFilePath).getParent());
            log.info("Dead-letter file will be written to: {}", this.deadLetterFilePath);
        } catch (IOException e) {
            log.error("Failed to create directory for dead-letter file: {}", this.deadLetterFilePath, e);
        } catch (Exception e) { // Catch other potential runtime errors during path manipulation
            log.error("Error initializing DeadLetterHandler with path: {}", this.deadLetterFilePath, e);
            // Consider re-throwing as a custom initialization exception if this is critical
            // throw new IllegalStateException("Failed to initialize DeadLetterHandler path", e);
        }
    }

    public synchronized void recordFailedChunk(String chunkIdentifier, List<String> lines, String reason, Throwable cause) {
        try {
            String header = String.format("[%s] Failed Chunk ID: %s, Reason: %s%n",
                    LocalDateTime.now().format(formatter), chunkIdentifier, reason);
            if(cause != null) {
                header += String.format("Cause: %s%n", cause.getMessage());
            }
            Files.writeString(Paths.get(deadLetterFilePath), header, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            for (String line : lines) {
                Files.writeString(Paths.get(deadLetterFilePath), line + System.lineSeparator(), StandardOpenOption.APPEND);
            }
            Files.writeString(Paths.get(deadLetterFilePath), "--- END OF CHUNK ---" + System.lineSeparator(), StandardOpenOption.APPEND);
        } catch (IOException e) {
            log.error("Failed to write to dead-letter file for chunk {}: {}", chunkIdentifier, e.getMessage(), e);
        }
    }
}
