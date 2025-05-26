package com.dev.pranay.Multithreaded.Batched.Processing.exceptions;

// 1. Custom Exception for Chunk Processing Failures

import java.util.ArrayList;
import java.util.List;

public class ChunkProcessingException extends RuntimeException {

    private final String chunkIdentifier; // e.g., "Chunk starting line X" or a UUID
    private final List<String> problematicLines;
    private final int attemptedRecordCount;
    private int successfullyProcessedCount;

    public ChunkProcessingException(String message, Throwable cause, String chunkIdentifier, List<String> problematicLines, int attemptedRecordCount, int successfullyProcessedCount) {
        super(message, cause);
        this.chunkIdentifier = chunkIdentifier;
        this.problematicLines = new ArrayList<>(problematicLines);
        this.attemptedRecordCount = attemptedRecordCount;
        this.successfullyProcessedCount = successfullyProcessedCount;
    }

    // Getters for these fields

    public String getChunkIdentifier() { return chunkIdentifier;}

    public List<String> getProblematicLines() { return problematicLines;}

    public int getAttemptedRecordCount() { return attemptedRecordCount;}

    public int getSuccessfullyProcessedCount() { return successfullyProcessedCount;}
}
