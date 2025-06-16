package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aStreamingSemaphore2 {

    private final ProductRepository productRepository;
    private final TransactionTemplate transactionTemplate;
    @Qualifier("batchExecutorService")
    private final ExecutorService executorService;
    private static final int BATCH_SIZE = 2000;
    private static final int MAX_PARALLEL_BATCHES = 50;
    private final Semaphore semaphore = new Semaphore(MAX_PARALLEL_BATCHES);

    @Transactional(readOnly = true)
    public String applyDiscountsInParallelWithStreaming() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        final List<Product> chunk = new ArrayList<>(BATCH_SIZE);

        log.info("Starting streaming discount processing.....");
        try (Stream<Product> productStream = productRepository.streamUnprocessedProducts()){

            productStream.forEach(product -> {
                chunk.add(product);

                if(chunk.size() == BATCH_SIZE) {
                    List<Product> batchToProcess = new ArrayList<>(chunk);
                    submitBatchForProcessing(futures, batchToProcess);
                    chunk.clear();
                }
            });
        }

        if(!chunk.isEmpty()) {
            log.info("Processing final partial chunk of size {}", chunk.size());
            submitBatchForProcessing(futures,new ArrayList<>(chunk));
            chunk.clear();
        }

        try {
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (Exception e) {
            log.error("Error waiting for batch processing to complete: {}", e.getMessage(), e);
        }

        log.info("Streaming discount processing completed successfully!");
        return "Streaming discount processing completed successfully!";
    }

    private void submitBatchForProcessing(List<CompletableFuture<Void>> futures, List<Product> batch) {
        try {
            semaphore.acquire();
            CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
                try {
                    processProductBatchWithRetries(batch);
                } finally {
                    semaphore.release();
                }
            }, executorService);
            futures.add(future);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.error("Thread interrupted while acquiring semaphore", e);
        }
    }

    private void processProductBatchWithRetries(List<Product> products) {
        int retryCount = 0;
        final int maxRetries = 3;
        boolean success = false;

        while (!success && retryCount < maxRetries) {
            try {
                processProductBatch(products);
                success = true;
            } catch (Exception e) {
                retryCount++;
                log.warn("Batch retry {}/{} due to error: {}", retryCount, maxRetries, e.getMessage());
                try {
                    Thread.sleep(200L);
                } catch (InterruptedException ex) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        if(!success) {
            log.error("Batch permanently failed after {} attempts", maxRetries);
        }
    }

    private void processProductBatch(List<Product> products) {
        transactionTemplate.execute(status -> {
            try {
                Instant start = Instant.now();
                log.info("Processing batch of size {} on thread {}", products.size(), Thread.currentThread().getName());

                products.forEach(this::updateDiscountedPrice);
                productRepository.saveAll(products);

                Instant end = Instant.now();
                log.info("Finished batch in {} ms", Duration.between(start, end).toMillis());
            } catch (Exception e) {
                log.error("Error in transaction: {}", e.getMessage(), e);
                status.setRollbackOnly();
            }
            return null;
        });
    }

    private void updateDiscountedPrice(Product product) {
        Double price = product.getPrice();
        double discountPercentage;

        if (price >= 2000.00) {
            discountPercentage = 10.0;
        } else if (price >= 1000.00) {
            discountPercentage = 5.0;
        } else {
            discountPercentage = 0.0;
        }

        if (discountPercentage > 0) {
            double priceAfterDiscount = price - (price * discountPercentage / 100);
            product.setOfferApplied(true);
            product.setDiscountPercentage(discountPercentage);
            product.setPriceAfterDiscount(priceAfterDiscount);
        } else {
            product.setOfferApplied(false);
            product.setDiscountPercentage(0.0);
            product.setPriceAfterDiscount(price);
        }

        product.setPostProcessed(true); //Mark as processed
    }
}
