package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.config.BatchProcessingMetrics;
import com.dev.pranay.Multithreaded.Batched.Processing.config.PaginationConfig;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
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

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

/**
 * Streaming has a nice flow and elegance for short or mid-sized jobs,
 * but chunked pagination is more robust and maintainable for long-running production workloads.
 */

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aChunkSemaphore {

    //production-ready Chunked Pagination + Multithreaded Processing + Retry Support
    //Phase 1 (Pagination, DLQ, Retries)

    private final ProductRepository productRepository;
    private final TransactionalBatchProcessor batchProcessor;
    private final BatchProcessingMetrics metrics; //Inject metrics component

    @Qualifier("postProcessingExecutor")
    private final ExecutorService executor;

    // --- Job Configuration ---
    // Limits how many batches can run in parallel to prevent overwhelming the system.
    private static final int MAX_CONCURRENT_BATCHES = 10;
    private final Semaphore backpressureSemaphore = new Semaphore(MAX_CONCURRENT_BATCHES);

    @Value("${batch.processing.maxRetries:3}")
    private int maxRetries;

    /**
     * Main entry point for the job. Orchestrates fetching pages and submitting them for parallel processing.
     * This method itself is not transactional; transactions are managed per-batch.
     */
    public String processAllWithPagination() {
        log.info("Starting paginated post-processing job. Max Concurrency: {}, Page Size: {}, Retries: {}",
                MAX_CONCURRENT_BATCHES, PaginationConfig.PAGE_SIZE, maxRetries);

        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        int pageNumber = 0;
        Page<Product> page;

        // The main loop fetches one page at a time until no more unprocessed data exists.
        do {
            final Pageable pageRequest = PageRequest.of(pageNumber++, PaginationConfig.PAGE_SIZE);
            page = productRepository.findUnprocessed(pageRequest);

            final List<Product> currentBatch = page.getContent();
            if(!currentBatch.isEmpty()) {
                submitBatchForProcessing(futures, currentBatch);
            }
        } while (page.hasNext());

        // Wait for all submitted tasks (and their retries) to complete.
        log.info("All {} batches have been submitted. Waiting for completion...", futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("Paginated post-processing completed successfully!");
        return "Chunked Pagination + Multithreaded Processing + Retry Support all Completed.";
    }


    /**
     * Safely submits a batch to the executor service, using a Semaphore to apply backpressure.
     * This prevents the service from creating too many tasks at once.
     */
    private void submitBatchForProcessing(List<CompletableFuture<Void>> futures, List<Product> batch) {
        try {
            // THIS IS THE BACKPRESSURE POINT
            // 1. Acquire Permit (Backpressure): Blocks if the max number of concurrent tasks is reached.
            backpressureSemaphore.acquire();
            log.trace("Semaphore permit acquired. Active tasks: ~{}", MAX_CONCURRENT_BATCHES - backpressureSemaphore.availablePermits());

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                Timer.Sample timerSample = metrics.startTimer(); //start timer
                try {
                    // 2. Process with Retry Logic
                    processBatchWithRetries(batch);
                } finally {
                    metrics.stopTimer(timerSample); //stop timer
                    // THIS RELEASES THE PRESSURE
                    // 3. CRITICAL: Always release the permit in a finally block to prevent deadlocks.
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
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
          try {
              // Delegate the actual work to the transactional component.
              // Each attempt will run in a completely new transaction.
              batchProcessor.processBatch(batch);
              metrics.incrementBatchesProcessed(); // Increment success counter
              return; // Success, exit the loop.
          } catch (Exception e) {
              log.warn("Attempt {}/{} failed for batch starting with product ID {}. Retrying... Error: {}",
                      attempt, maxRetries, batch.get(0).getId(), e.getMessage());

              if(attempt == maxRetries) {
                  metrics.incrementBatchesFailed(); //increment failure counter
                  // DEAD-LETTER QUEUE: The batch has permanently failed. Log the IDs for manual review.
                  List<Long> failedIds = batch.stream()
                          .map(product -> product.getId())
                          .collect(Collectors.toList());
                  log.error("Batch permanently failed after {} attempts. Product IDs for manual investigation: {}", maxRetries, failedIds, e);

                  // We do NOT re-throw the exception, as that would kill the CompletableFuture's thread.
                  // The error is logged, and the overall job can continue with other batches.

                  /*
                  This code block is the DLQ because it takes the "dead" items (the failedIds)
                  and moves them to a separate destination (the error logs) for manual review,
                  preventing them from stopping the entire job.
                   */
              } else {
                  try {
                      // Exponential backoff: wait longer after each failure.
                      Thread.sleep(1000L * attempt);
                  } catch (InterruptedException ex) {
                      Thread.currentThread().interrupt();
                  }
              }
          }
        }
    }
}


/**
 *  the current implementation of ProductPostProcessor does not take care of checkpointing
 *  for resumability.
 *
 * Here’s why:
 *
 * It Always Starts from the Beginning: Every time the processAllWithPagination() method is called,
 * it initializes int pageNumber = 0; and starts fetching records from the very first page.
 *
 * No Persistent State: The current pageNumber is just a local variable held in memory.
 * If the application crashes or is restarted halfway through the job (say, at page 500),
 * that progress is lost. When the job runs again, it will start over from page 0.
 *
 * While the postProcessed = true flag on each product prevents the business logic from
 * being applied twice to the same record (which is great for idempotency),
 * the job still has to waste significant time and resources re-fetching and skipping over
 * the hundreds of pages it has already successfully completed.
 *
 * A true checkpoint mechanism would involve persisting the last successfully completed
 * page number (or last product ID for keyset pagination) to a database table or a file,
 * and reading from it at the start of the job to resume from where it left off.
 */


/**
 * why do we need checkpoint despite having postProcessed flag?
 *
 *
 *We've hit on the key distinction between application memory and persistent database storage.
 *
 * The job will still run from the start because the variable that keeps track of the
 * current page number (pageNumber) exists only in our application's memory.
 *
 * Here’s the sequence of events when a crash happens without a checkpoint:
 *
 * Job is Running: Your ProductPostProcessor is happily running.
 * The pageNumber variable in memory has a value of 500.
 * Application Crashes: The application process is terminated unexpectedly.
 * Everything in the application's memory, including the pageNumber variable, is completely erased.
 * It's gone forever.
 * Application Restarts: You start the application again.
 * Job is Triggered: The processAllWithPagination() method is called.
 * Re-initialization: The code executes this line again from scratch:
 * Java
 * int pageNumber = 0;
 * The application has no memory of the previous run, so it initializes pageNumber back to
 * its starting value of 0.
 * The postProcessed flag is different because it's stored in the database, which is persistent.
 * The database survives the crash.
 *
 * So, while the database remembers which individual products are done,
 * the application itself forgets which page it was on.
 * This forces the application to start its loop from page 0 and re-read all 500 pages,
 * even if it ends up doing no work with the products on those pages.
 * This is the Wasted Work.
 *
 * The postProcessed flag is for Correctness.
 * It's a safety net at the record level to guarantee an item is never discounted twice,
 * no matter what. It ensures the final state of your data is correct.
 *
 * The JobCheckpoint is for Efficiency.
 * It's a performance optimization at the job level to prevent wasting time and resources
 * after a crash. It ensures the recovery process is fast.
 *
 * we need both for a truly production-grade system: one to ensure we don't do the wrong thing,
 * and the other to ensure you don't do the right thing over and over again unnecessarily.
 */


