package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.config.BatchProcessingMetrics;
import com.dev.pranay.Multithreaded.Batched.Processing.config.PaginationConfig;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.JobCheckpoint;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.JobCheckpointRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import io.micrometer.core.instrument.Timer;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
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
public class ProductServiceV2aChunkSemaphoreWithCheckPoint {

    //Dependencies

    private final ProductRepository productRepository;
    private final TransactionalBatchProcessor batchProcessor;
    private final BatchProcessingMetrics metrics;
    private final JobCheckpointRepository checkpointRepository;

    private static final String JOB_NAME = "DISCOUNT_PROCESSING_JOB"; //give a unique job name

    @Qualifier("postProcessingExecutor")
    private final ExecutorService executor;

    //job configuration
    private final Semaphore backpressureSemaphore = new Semaphore(10);

    @Value("${batch.processing.maxRetries:3}")
    private int maxRetries;

    /**
     * Main entry point for the job. Reads the last checkpoint and orchestrates fetching pages
     * and submitting them for parallel processing.
     */

    @Transactional // Add transactionality to the main method for checkpoint operations
    public String processAllWithPagination() {
        //Load the checkpoint at the start of the job

        int startingPage = loadCheckpoint();

        log.info("Starting paginated post-processing job from page {}. Max Concurrency: {}, Page Size: {}, Retries: {}",
                startingPage, 10, PaginationConfig.PAGE_SIZE, maxRetries);

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        int currentPageNumber = startingPage;
        Page<Product> page;

        do {
            final Pageable pageRequest = PageRequest.of(currentPageNumber, PaginationConfig.PAGE_SIZE);
            page = productRepository.findUnprocessed(pageRequest);

            final List<Product> currentBatch = page.getContent();
            if(!currentBatch.isEmpty()) {
                submitBatchForProcessing(futures, currentBatch);
                //Save checkpoint after successfully submitting a page
                saveCheckpoint(currentPageNumber);
            }
            currentPageNumber++;

        } while (page.hasNext());

        log.info("All {} batches have been submitted. Waiting for completion...", futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("Paginated post-processing completed successfully!");
        return "Completed.";
    }

    private int loadCheckpoint() {
        return checkpointRepository.findById(JOB_NAME)
                .map(JobCheckpoint::getLastProcessedPage)
                .orElse(0);
    }

    private void saveCheckpoint(int pageNumber) {
        JobCheckpoint checkpoint = new JobCheckpoint(JOB_NAME, pageNumber);
        checkpointRepository.save(checkpoint);
        log.trace("Checkpoint saved for page {}", pageNumber);
    }

    /**
     * Safely submits a batch to the executor service, using a Semaphore to apply backpressure.
     * This prevents the service from creating too many tasks at once.
     */

    private void submitBatchForProcessing(List<CompletableFuture<Void>> futures, List<Product> batch) {
        try {
            backpressureSemaphore.acquire();
            log.trace("Semaphore permit acquired. Active tasks: ~{}", 10 - backpressureSemaphore.availablePermits());

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                Timer.Sample timerSample = metrics.startTimer();
                try {
                    processBatchWithRetries(batch);
                } finally {
                    metrics.stopTimer(timerSample);
                    backpressureSemaphore.release();
                    log.trace("Semaphore permit released.");
                }
            }, executor);

            futures.add(future);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread was interrupted while waiting to acquire semaphore permit. Batch may not have been submitted.", e);
        }
    }

    /**
     * A wrapper that adds resilience by retrying a batch if it fails due to a transient error
     * (e.g., optimistic lock exception, temporary network issue).
     */
    private void processBatchWithRetries(List<Product> batch) {
        for(int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                batchProcessor.processBatch(batch);
                metrics.incrementBatchesProcessed();
                return; //success exit for the loop
            } catch (Exception e) {
                log.warn("Attempt {}/{} failed for batch starting with product ID {}. Retrying... Error: {}",
                        attempt, maxRetries, batch.get(0).getId(), e.getMessage());

                if(attempt == maxRetries) {
                    metrics.incrementBatchesFailed();
                    List<Long> failedIds = batch.stream().map(id -> id.getId()).collect(Collectors.toList());
                    log.error("Batch permanently failed after {} attempts. Product IDs for manual investigation: {}", maxRetries, failedIds, e);
                } else {
                    try {
                        Thread.sleep(1000L * attempt);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }


}


