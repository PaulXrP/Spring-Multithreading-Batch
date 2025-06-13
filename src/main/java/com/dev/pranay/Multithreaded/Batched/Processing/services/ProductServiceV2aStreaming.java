package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Stream;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aStreaming { //In-App Streaming (The Immediate Improvement)

    private final ProductRepository productRepository;
    private final TransactionTemplate transactionTemplate;
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private static final int BATCH_SIZE = 2000;

    /*
     * This is the refactored main method.
     * It streams products directly from the database, collects them into chunks,
     * and processes these chunks in parallel without ever holding all products in memory.
     *
     * Add @Transactional(readOnly = true) here.
     * This keeps the database connection open for the entire duration of the try-with-resources block,
     * allowing the stream to be fully consumed. readOnly is a performance optimization.
     */
    @Transactional(readOnly = true)
    public String applyDiscountsInParallelWithStreaming() {
        // List to hold the futures for parallel execution
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        // Temporary list to build each chunk
        final List<Product> chunk = new ArrayList<>(BATCH_SIZE);

        log.info("Starting streaming discount processing.....");

        // Use try-with-resources to ensure the stream and database cursor are always closed
        try (Stream<Product> productStream = productRepository.streamAllProducts()){

            productStream.forEach(product -> {
                chunk.add(product);
                // When the chunk is full, submit it for processing
                if(chunk.size() == BATCH_SIZE) {
                    // We must pass a copy of the chunk, as the original 'chunk' list will be cleared.
                    List<Product> batchToProcess = new ArrayList<>(chunk);
                    futures.add(CompletableFuture.runAsync(
                            () -> processProductBatch(batchToProcess), executorService
                    ));
                    chunk.clear();
                }
            });

        }  // The stream and DB resources are automatically closed here.

        // After the stream is exhausted, process the final partial chunk if it exists
        if(!chunk.isEmpty()) {
            log.info("Processing the final partial chunk of size {}", chunk.size());
            futures.add(CompletableFuture.runAsync(
                    () -> processProductBatch(chunk), executorService
            ));
            chunk.clear();
        }

        // Wait for all the parallel batch processing tasks to complete
        log.info("All {} batches submitted. Waiting for completion...", futures.size());
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        log.info("Streaming discount processing completed successfully!");
        return "Streaming discount processing completed successfully!";
    }


    /**
     * This batch processing method is now MORE EFFICIENT.
     * It no longer needs to fetch products from the DB because they are passed in directly from the stream.
     *
     * @param products The list of product entities for the batch.
     */
    private void processProductBatch(List<Product> products) {
        // Each batch runs in its own dedicated transaction
        transactionTemplate.execute(status -> {
            try {
                log.info("Processing batch of size {} on thread {}", products.size(), Thread.currentThread().getName());

                // STEP 1: This DB call is now REMOVED, making the process faster
                // List<Product> products = productRepository.findAllById(batchProductIds); <-- NO LONGER NEEDED

                // STEP 2: Apply business logic to each entity
                products.forEach(this::updateDiscountedPrice);

                // STEP 3: Save the updated entities. This will use Hibernate batching.
                productRepository.saveAll(products);
            } catch (Exception e) {
                log.error("Error processing batch on thread {}: {}", Thread.currentThread().getName(), e.getMessage(), e);
                status.setRollbackOnly(); //Mark transaction for rollback on error
            }
            return null; // Return value for the TransactionCallback
        });
    }

    // The core business logic remains the same.
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
    }

}
