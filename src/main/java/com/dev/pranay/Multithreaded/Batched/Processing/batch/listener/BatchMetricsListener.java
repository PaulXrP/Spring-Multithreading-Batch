package com.dev.pranay.Multithreaded.Batched.Processing.batch.listener;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.batch.core.ChunkListener;
import org.springframework.batch.core.scope.context.ChunkContext;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

/**
 * A listener dedicated to capturing metrics about chunk processing using Micrometer.
 * It provides deep insight into the job's performance by tracking chunk duration
 * and the number of items processed. This component is decoupled from the core
 * business logic.
 */

@Component
public class BatchMetricsListener implements ChunkListener {

    // A thread-safe way to store the start time for each chunk.
    // This is necessary because the step is multi-threaded, and a simple instance
    // variable would lead to race conditions.

    private final ThreadLocal<Long> chunkStartTime = new ThreadLocal<>();

    private final Timer chunkProcessingTimer;
    private final Counter itemsProcessedCounter;

    /**
     * The constructor where we define our metrics.
     * @param meterRegistry The Micrometer registry, provided by Spring Boot Actuator.
     */
    public BatchMetricsListener(MeterRegistry meterRegistry) {
        // Create a timer to record how long each chunk takes to process (Read-Process-Write).
        // Tags are added to allow for powerful filtering and dashboarding in monitoring tools.
        this.chunkProcessingTimer = meterRegistry.timer("spring.batch.chunk.duration", "jobName", "discountProcessingJob");

        // Create a counter to track the total number of items successfully processed across all chunks.
        this.itemsProcessedCounter = meterRegistry.counter("spring.batch.items.processed", "jobName", "discountProcessingJob");
    }

    @Override
    public void beforeChunk(ChunkContext context) {
        // Before a chunk starts, record the current time in nanoseconds.
        chunkStartTime.set(System.nanoTime());
    }

    @Override
    public void afterChunk(ChunkContext context) {
        // After a chunk successfully completes, calculate the duration.
        long duration = System.nanoTime() - chunkStartTime.get();
        chunkProcessingTimer.record(duration, TimeUnit.NANOSECONDS);


        // Get the number of items that were read in this specific chunk.
        // The read count is a reliable measure of items processed in a successful chunk.
        int itemsInChunk = Math.toIntExact(context.getStepContext().getStepExecution().getReadCount());
        itemsProcessedCounter.increment(itemsInChunk);

        // It's crucial to remove the value from the ThreadLocal to prevent memory leaks.
        chunkStartTime.remove();
    }

    @Override
    public void afterChunkError(ChunkContext context) {
        // If an error occurs that isn't skipped, we still need to clean up the ThreadLocal.
        chunkStartTime.remove();
        // Optionally, we could add another counter here to track chunk failures.

    }
}
