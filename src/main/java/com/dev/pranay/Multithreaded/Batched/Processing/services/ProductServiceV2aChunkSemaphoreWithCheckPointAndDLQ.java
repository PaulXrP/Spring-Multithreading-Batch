package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.config.BatchProcessingMetrics;
import com.dev.pranay.Multithreaded.Batched.Processing.config.PaginationConfig;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.JobCheckpoint;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.DeadLetterRecordRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.JobCheckpointRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aChunkSemaphoreWithCheckPointAndDLQ {

    //Dependencies
    private final ProductRepository productRepository;
    private final TransactionalBatchProcessor batchProcessor;
    private final DeadLetterService deadLetterService;
    private final JobCheckpointRepository checkpointRepository;
    private final BatchProcessingMetrics metrics;

    @Qualifier("postProcessingExecutor")
    private final ExecutorService executor;

    //job configuration
    private static final String JOB_NAME = "DISCOUNT_PROCESSING_JOB";

    @Value("${batch.processing.maxConcurrent:10}")
    private int maxConcurrentBatches;

    @Value("${batch.processing.maxRetries:3}")
    private int maxRetries;

    private Semaphore backpressureSemaphore;

    /**
     * Main entry point for the job. Orchestrates checkpointing, pagination, and parallel processing.
     */

    @Transactional // Manages transactions for checkpoint operations.
    public String processAllProducts() {
        backpressureSemaphore = new Semaphore(maxConcurrentBatches);
        int startingPage = loadCheckpoint();

        log.info("Starting paginated post-processing job '{}'. StartPage: {}, MaxConcurrency: {}, PageSize: {}, Retries: {}",
                JOB_NAME, startingPage, maxConcurrentBatches, PaginationConfig.PAGE_SIZE, maxRetries);

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        int currentPageNumber =  startingPage;
        Page<Product> page;

        do {
            final Pageable pageRequest = PageRequest.of(currentPageNumber, PaginationConfig.PAGE_SIZE);
            page = productRepository.findUnprocessed(pageRequest);

            List<Product> currentBatch = page.getContent();
            if(!currentBatch.isEmpty()) {
                submitBatchForProcessing(futures, currentBatch, currentPageNumber);
            }
            currentPageNumber++;

        } while (page.hasNext());

        log.info("All {} pages have been submitted. Waiting for completion...", futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("Paginated post-processing job '{}' completed successfully!", JOB_NAME);
        return "Completed.";
    }

    /**
     *  Submits a batch for processing and atomically links checkpointing to its successful completion.
     */
    private void submitBatchForProcessing(List<CompletableFuture<Void>> futures, List<Product> batch, int pageNumber) {
        try {
            backpressureSemaphore.acquire();
            log.trace("Semaphore permit acquired for page {}. Active tasks: ~{}", pageNumber, maxConcurrentBatches - backpressureSemaphore.availablePermits());

            CompletableFuture<Void> future = CompletableFuture
                    .runAsync(() -> {
                        Timer.Sample timerSample = metrics.startTimer();
                        try {
                            processBatchWithRetries(batch);
                        } finally {
                            metrics.stopTimer(timerSample);
                        }
                    }, executor)
                    .thenRun(() -> {
                        // This block only executes if runAsync() completes WITHOUT an exception.
                        // This is where we safely save the checkpoint.
                        saveCheckpoint(pageNumber);
                    })
                    .whenComplete((result, ex) -> {
                        // This block runs on success OR failure, perfect for releasing the semaphore.
                        if(ex != null) {
                            log.error("Async processing for page {} failed permanently. See previous logs for DLQ details.", pageNumber, ex.getCause());
                        }
                        backpressureSemaphore.release();
                        log.trace("Semaphore permit released for page {}.", pageNumber);
                    });

            futures.add(future);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread was interrupted while acquiring semaphore for page {}. Batch will not be processed.", pageNumber, e);
        }
    }



    /**
     * Now calls the DeadLetterService on permanent failure and propagates an exception.
     */
    private void processBatchWithRetries(List<Product> batch) {
        for(int attempt =  1; attempt <=maxRetries; attempt++) {
            try {
                batchProcessor.processBatch(batch);
                metrics.incrementBatchesProcessed();
                return; //Success, exit the loop
            } catch (Exception e) {
                log.warn("Attempt {}/{} failed for batch starting with product ID {}. Retrying... Error: {}",
                        attempt, maxRetries, batch.isEmpty() ? "N/A" : batch.get(0).getId(), e.getMessage());

                if (attempt == maxRetries) {
                    metrics.incrementBatchesFailed();
                    List<Long> failedIds = batch.stream().map(product -> product.getId()).collect(Collectors.toList());

                    // **DLQ**: Formalize failure by writing to the DLQ.
                    deadLetterService.recordFailures(JOB_NAME, failedIds, e.getMessage());

                    // **CRITICAL**: Throw an exception to signal the permanent failure to the CompletableFuture.
                    // This prevents the .thenRun() checkpointing block from executing.
                    throw new RuntimeException("Batch failed permanently after " + maxRetries + " attempts.", e);
                } else {
                    try {
                        Thread.sleep(1000L * attempt); // Simple linear backoff
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private int loadCheckpoint() {
        return checkpointRepository.findById(JOB_NAME)
                .map(JobCheckpoint::getLastProcessedPage)
                .map(lastPage -> lastPage + 1) //Resume from the page after the last successful one.
                .orElse(0);
    }

    private void saveCheckpoint(int successfullyProcessedPage) {
        checkpointRepository.save(new JobCheckpoint(JOB_NAME, successfullyProcessedPage));
        log.info("Checkpoint saved for job '{}'. Last processed page: {}", JOB_NAME, successfullyProcessedPage);
    }
}

/**
 * Refine Checkpoint Atomicity
 * The Current Problem:
 * our current logic is:
 *
 * Fetch page N.
 * Submit page N to the ExecutorService.
 * Immediately save the checkpoint for page N.
 * The CompletableFuture for page N runs in the background.
 * Consider this failure scenario:
 *
 * The checkpoint for page 5 is saved successfully.
 * The application crashes before the CompletableFuture for page 5 actually finishes its
 * processing and commits the transaction.
 * When you restart the job, loadCheckpoint() will return 5. The job will start processing
 * from page 6, skipping the records from page 5 which were never actually committed to the
 * database.
 *
 * The Solution:
 * The checkpoint for a page should only be saved after that page's batch has been successfully
 * processed and committed. We need to tie the saveCheckpoint action to the successful completion
 * of the CompletableFuture.
 */

/**
 * Formalize the Dead Letter Queue
 *
 * The previous State:  log the failed IDs. This is good for a human operator.
 * The Next Level: Persist the failed information to a database table or a dedicated file.
 * This creates an auditable, queryable record of failures that can be used for automated
 * reprocessing late
 */
