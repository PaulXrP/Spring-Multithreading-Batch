package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aStreamingSemaphore {

    // Final dependencies injected via constructor
    private final ProductRepository productRepository;
    private final TransactionTemplate transactionTemplate;
    @Qualifier("batchExecutorService") // Specify which ExecutorService bean to use
    private final ExecutorService executorService;

    // --- Configuration for the batch job ---
    private static final int BATCH_SIZE = 2000;
    // Limits how many batches run in parallel to prevent overwhelming memory or the DB connection pool.
    private static final int MAX_CONCURRENT_BATCHES = 10;
    private final Semaphore backpressureSemaphore = new Semaphore(MAX_CONCURRENT_BATCHES);

    /**
     * Main entry point for the job. Orchestrates streaming, chunking, and parallel processing.
     * Marked as read-only transactional to keep the DB connection alive for the stream.
     */
    @Transactional(readOnly = true)
    public String applyDiscounts() {
        log.info("Starting production-grade discount processing job. Max Concurrency: {}", MAX_CONCURRENT_BATCHES);
        final List<CompletableFuture<Void>> futures = new ArrayList<>();
        final List<Product> chunk = new ArrayList<>(BATCH_SIZE);

        // try-with-resources ensures the database stream is always closed, even if errors occur.
        try(Stream<Product> productStream = productRepository.streamUnprocessedProducts()) {
            productStream.forEach(product -> {
                chunk.add(product);
                if(chunk.size() == BATCH_SIZE) {
                    submitBatchForProcessing(futures, new ArrayList<>(chunk));
                    chunk.clear();
                }
            });
        }

        // Process the final, potentially smaller, chunk.
        if(!chunk.isEmpty()) {
            log.info("Submitting final partial chunk of size {}.", chunk.size());
            submitBatchForProcessing(futures, new ArrayList<>(chunk));
            chunk.clear();
        }

        // Wait for all dispatched jobs to complete.
        log.info("All {} batches have been submitted. Waiting for completion...", futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("Streaming discount processing completed successfully!");
        return "Streaming discount processing completed successfully!";
    }

    /**
     * Safely submits a batch to the executor service, using a Semaphore to apply backpressure.
     * This prevents the service from launching too many tasks at once.
     */
    private void submitBatchForProcessing(List<CompletableFuture<Void>> futures, List<Product> batch) {
        try {
            // Acquire a permit. If no permits are available, this thread will block until one is released.
            // This is the backpressure mechanism.

            backpressureSemaphore.acquire();
            log.trace("Semaphore permit acquired. Active tasks: ~{}", MAX_CONCURRENT_BATCHES - backpressureSemaphore.availablePermits());

            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    processBatchWithRetries(batch);
                } finally {
                    // CRITICAL: Always release the permit in a 'finally' block to prevent deadlocks.
                    backpressureSemaphore.release();
                    log.trace("Semaphore permit released.");
                }
            }, executorService);
            futures.add(future);
        } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
            log.error("Thread interrupted while waiting to submit a batch.", e);
        }
    }

    /**
     * A wrapper that adds resilience by retrying a batch if it fails due to a transient error.
     */

    private void processBatchWithRetries(List<Product> products) {
        final int maxRetries = 3;
        int attempt = 0;

        while (attempt < maxRetries) {
            try {
                // Each attempt runs in its own new transaction via TransactionTemplate.
                processSingleBatchInTransaction(products);
                return; // Success, exit the loop.
            } catch (Exception e) {
                attempt++;
                log.warn("Batch processing failed on attempt {}. Error: {}. Retrying...", attempt, e.getMessage());
                if(attempt >= maxRetries) {
                    log.error("Batch permanently failed after {} attempts for products starting with ID {}.",
                            maxRetries, products.isEmpty() ? "N/A" : products.get(0).getId(), e);
                    // Optionally, add logic here to write the failed batch IDs to a "dead letter" log/table.

                } else {
                    try {
                        Thread.sleep(500 * attempt); // Exponential backoff
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

        }
    }

    /**
     * Processes a single batch within a new, dedicated transaction.
     */
    private void processSingleBatchInTransaction(List<Product> products) {
        transactionTemplate.execute(status -> {
            log.debug("Processing batch of size {} on thread {}", products.size(), Thread.currentThread().getName());
            products.forEach(this::updateDiscountAndMarkProcessed);
            // saveAll will be optimized by Hibernate's JDBC batching settings
            productRepository.saveAll(products);
            return null;
        });
    }

    /**
     * The core business logic. It applies the discount rules and, crucially,
     * marks the product as processed to ensure idempotency.
     */
    private void updateDiscountAndMarkProcessed(Product product) {
        double price = product.getPrice();
        double discountPercentage;

        if (price >= 2000.00) { discountPercentage = 10.0; }
        else if (price >= 1000.00) { discountPercentage = 5.0; }
        else { discountPercentage = 0.0; }

        if (discountPercentage > 0) {
            product.setOfferApplied(true);
            product.setDiscountPercentage(discountPercentage);
            product.setPriceAfterDiscount(price - (price * discountPercentage / 100));
        } else {
            product.setOfferApplied(false);
            product.setDiscountPercentage(0.0);
            product.setPriceAfterDiscount(price);
        }

        // This is the critical flag that makes the job restartable.
        product.setPostProcessed(true);
    }
}

/*
The new ProductServiceV2aStreamingSemaphore trades a small amount of theoretical,
best-case-scenario speed for an enormous gain in stability, reliability, and predictability.
The Semaphore doesn't make it faster; it makes it possible to run at scale without crashing.
It's the difference between a prototype and a professional-grade tool.
 */
