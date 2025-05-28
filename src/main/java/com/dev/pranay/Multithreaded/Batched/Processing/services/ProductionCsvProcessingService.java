package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.exceptions.ChunkProcessingException;
import com.dev.pranay.Multithreaded.Batched.Processing.utilities.DeadLetterHandler;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.Counter;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.*;


@Service
@Slf4j
public class ProductionCsvProcessingService {

    private final EntityManagerFactory emf;
    private final DeadLetterHandler deadLetterHandler;

    //Metrics

    private final Counter totalLinesReadCounter;
    private final Counter chunksSubmittedCounter;
    private final Counter chunksSuccessfullyProcessedCounter;
    private final Counter chunksFailedProcessingCounter;
    private final Counter recordsPersistedCounter;
    private final Timer chunkProcessingTimer;
    private final Timer totalJobTimer;

    // Configuration
    private final int chunkSize;
    private final int threadPoolSize;
    private final int queueCapacity;
    private final long executorTerminationTimeoutHours;
    private final String deadLetterBaseFileName;

    private ExecutorService executorService;

    public ProductionCsvProcessingService(
            EntityManagerFactory emf,
            MeterRegistry meterRegistry,
            @Value("${csv.processing.chunkSize:2000}") int chunkSize,
            @Value("${csv.processing.threadPoolSize:4}")  int threadPoolSize,
            @Value("${csv.processing.queueCapacity:100}")  int queueCapacity, //for bounded queue
            @Value("${csv.processing.executorTerminationTimeoutHours:1}") long executorTerminationTimeoutHours,
            @Value("${csv.processing.deadLetterBaseFileName:failed_csv_chunks}") String deadLetterBaseFileName

    ) {
        this.emf = emf;
        this.chunkSize = chunkSize;
        this.threadPoolSize = threadPoolSize;
        this.queueCapacity = queueCapacity;
        this.executorTerminationTimeoutHours = executorTerminationTimeoutHours;
        this.deadLetterBaseFileName = deadLetterBaseFileName;
        this.deadLetterHandler = new DeadLetterHandler(this.deadLetterBaseFileName);

        // Initialize Metrics
        this.totalLinesReadCounter = meterRegistry.counter("csv.processing.lines.read");
        this.chunksSubmittedCounter = meterRegistry.counter("csv.processing.chunks.submitted");
        this.chunksSuccessfullyProcessedCounter = meterRegistry.counter("csv.processing.chunks.success");
        this.chunksFailedProcessingCounter = meterRegistry.counter("csv.processing.chunks.failed");
        this.recordsPersistedCounter = meterRegistry.counter("csv.processing.records.persisted");
        this.chunkProcessingTimer = meterRegistry.timer("csv.processing.chunk.duration");
        this.totalJobTimer = meterRegistry.timer("csv.processing.job.duration");


        log.info("ProductionCsvProcessingService initialized. Default ThreadPoolSize: {}, QueueCapacity: {} will be used per job.",
                this.threadPoolSize, this.queueCapacity);

    }

    public String loadCsvStreamingInChunksProduction(String filePath) {

        // Initialize ExecutorService with bounded queue for backpressure
        ThreadFactory namedThreadFactory = new ThreadFactoryBuilder().setNameFormat("csv-processor-%d").build();
        this.executorService = new ThreadPoolExecutor(
                this.threadPoolSize, //core pool size
                this.threadPoolSize, //maximum pool size
                0L, TimeUnit.MILLISECONDS, //keepAliveTime
                new LinkedBlockingDeque<>(this.queueCapacity), // Bounded work queue
                namedThreadFactory,
                new ThreadPoolExecutor.CallerRunsPolicy() // Policy for when queue is full (caller thread executes task)
                // Other options: AbortPolicy (throws RejectedExecutionException)
        );
        log.info("Created new ExecutorService for this CSV processing run: {}", this.executorService);


        Timer.Sample jobTimerSample = Timer.start();
        log.info("Starting CSV processing from file: {}", filePath);

        List<Future<?>> futures = new ArrayList<>();
        List<String> chunkBuffer = new ArrayList<>(chunkSize);
        int linesReadForCurrentChunk = 0;
        boolean isFirstLine = true; //To skip header

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))){
             String line;
             while ((line = br.readLine()) != null) {
                 totalLinesReadCounter.increment();
                 if(isFirstLine) {
                     isFirstLine = false;
                     log.info("Skipping header line: {}", line);
                     continue;
                 }

                 chunkBuffer.add(line);
                 linesReadForCurrentChunk++;

                 if(linesReadForCurrentChunk == chunkSize) {
                     submitChunk(new ArrayList<>(chunkBuffer), futures); //Submit a copy
                     chunkBuffer.clear();
                     linesReadForCurrentChunk = 0;
                 }
            }

            // Submit any remaining lines in the buffer as the final chunk
            if(!chunkBuffer.isEmpty()) {
                submitChunk(new ArrayList<>(chunkBuffer), futures);
            }

            log.info("All chunks submitted. Waiting for processing to complete. Total futures: {}", futures.size());

            // Wait for all tasks to complete and handle results/errors
            int totalSuccessfullyPersistedRecordsThisJob = 0;
            for(Future<?> future : futures) {
                try {
                    future.get(); //wait for the completion
                    // If future.get() doesn't throw, assume success for the chunk based on ProductBatchInserter's logic
                    chunksSuccessfullyProcessedCounter.increment();
                } catch (ExecutionException e) {
                    chunksFailedProcessingCounter.increment();
                    Throwable cause = e.getCause();
                    if(cause instanceof ChunkProcessingException cpe) {
                        log.error("Chunk processing failed: ID [{}], Message: {}. Problematic lines: {}",
                                cpe.getChunkIdentifier(), cpe.getMessage(), cpe.getProblematicLines(), cause);
                        deadLetterHandler.recordFailedChunk(cpe.getChunkIdentifier(), cpe.getProblematicLines(), cpe.getMessage(), cause);
                        recordsPersistedCounter.increment(cpe.getSuccessfullyProcessedCount()); //count partial success
                        totalSuccessfullyPersistedRecordsThisJob += cpe.getSuccessfullyProcessedCount();
                    } else {
                        log.error("An unexpected error occurred in a processing task.", cause);
                        deadLetterHandler.recordFailedChunk("UNKNOWN_CHUNK", List.of("Chunk failed with generic ExecutionException"),
                                "Generic task execution error", cause);
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("Main thread interrupted while waiting for a chunk to complete.", e);
                    chunksFailedProcessingCounter.increment(); //count as failed
                }
            }
            recordsPersistedCounter.increment(totalSuccessfullyPersistedRecordsThisJob); // This logic for counting successes needs refinement if tasks don't return counts

        } catch (RejectedExecutionException e) {
            log.error("CSV processing task rejected. The processing queue might be full. Consider increasing queue capacity or slowing down submission.", e);
            return "JOB FAILED: Task submission rejected, queue likely full.";
        } catch (IOException e) {
            log.error("Failed to read CSV file: {}. Error: {}", filePath, e.getMessage(), e);
            return "JOB FAILED: Error reading CSV file.";
        } finally {
            gracefulShutdownExecutor();
            long durationMillis = jobTimerSample.stop(totalJobTimer);
            log.info("CSV processing job finished in {} ms. Check logs for details on successes and failures.", durationMillis / 1_000_000.0); // Convert ns to ms
        }
        return "CSV processing job completed. See logs for status.";
    }

    private void gracefulShutdownExecutor() {
        log.info("Attempting to shut down executor service gracefully...");
        executorService.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!executorService.awaitTermination(executorTerminationTimeoutHours, TimeUnit.HOURS)) {
                log.warn("Executor service did not terminate within {} hours. Forcing shutdown...", executorTerminationTimeoutHours);
                executorService.shutdown(); //Cancel currently executing tasks
                //Wait a while for tasks to respond to being cancelled
                if(!executorService.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.error("Executor service did not terminate even after force.");
                }
            }
        } catch (InterruptedException e) {
            // (Re-)Cancel if current thread also interrupted
            executorService.shutdown();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
        log.info("Executor service shutdown completed.");
    }

    private void submitChunk(List<String> chunkToProcess, List<Future<?>> futures) {
        if(chunkToProcess.isEmpty()) return;

        // Use the instance variable executorService, which is now set per-call
        if(this.executorService == null || this.executorService.isShutdown()) {
            log.error("ExecutorService not available or shutdown when trying to submit chunk. Aborting chunk submission.");
            // Optionally, throw an exception or handle this as a failed chunk directly
            chunksFailedProcessingCounter.increment();
            deadLetterHandler.recordFailedChunk("UNSUBMITTED_CHUNK_" + UUID.randomUUID().toString().substring(00,8),
                    chunkToProcess, "Executor not available", null);
            return;
        }

        String chunkId = "chunk_" + UUID.randomUUID().toString().substring(0,8) + "_lines_" + chunkToProcess.size();
        log.info("Submitting chunk {} of size {} to process.", chunkId, chunkToProcess.size());

        Runnable task = () -> {
            Timer.Sample chunkTimerSample = Timer.start();
            EntityManager em = null;
            EntityTransaction tx = null;
            try {
                em = emf.createEntityManager();
                tx = em.getTransaction();
                tx.begin();
                new ProductBatchInserter(chunkToProcess, em).run();
                int successfullyProcessedInThisChunk = chunkToProcess.size();
                if (em.isJoinedToTransaction()) em.flush(); //// Flush remaining entities
                tx.commit();
                log.info("Chunk {} ({} lines) committed successfully.", chunkId, chunkToProcess.size());
                recordsPersistedCounter.increment(successfullyProcessedInThisChunk);
            } catch (ChunkProcessingException cpe) { //// Expected exception from inserter
                if (tx != null && tx.isActive()) {
                    try {
                        tx.rollback();
                        log.warn("Chunk {} rolled back due to processing error: {}", chunkId, cpe.getMessage());
                    } catch (Exception ex) {
                        log.error("Chunk {} rollback failed after processing error.", chunkId, ex);
                    }
                }
                throw cpe;
            } catch (Exception e) { //// Catch any other unexpected exception from the task
                if (tx != null && tx.isActive()) {
                    try {
                        tx.rollback();
                        log.warn("Chunk {} rolled back due to unexpected error: {}", chunkId, e.getMessage());
                    } catch (Exception ex) {
                        log.error("Chunk {} rollback failed after an unexpected error.", chunkId, ex);
                    }
                }
                // Wrap in ChunkProcessingException to be handled consistently by Future.get()
                throw new ChunkProcessingException("Unexpected error in chunk " + chunkId, e, chunkId, chunkToProcess, chunkToProcess.size(), 0);
            } finally {
                if(em != null && em.isOpen()) {
                    em.close();
                }
                chunkTimerSample.stop(chunkProcessingTimer);
            }
        };
        futures.add(this.executorService.submit(task)); //Use the instance executor
        chunksSubmittedCounter.increment();
        }
}

/*
When to Scale Further?

| Feature                    | When to Use                                  |
| -------------------------- | -------------------------------------------- |
| **ThreadPoolExecutor**     | Default for most scalable use cases          |
| **@Async + Spring Events** | For decoupled background processing          |
| **Kafka or Queue**         | If CSV upload is user-triggered & async      |
| **Spring Batch**           | If job restartability and job history matter |
| **Apache Spark/Flink**     | If you cross 10M+ records and go distributed |

 */
