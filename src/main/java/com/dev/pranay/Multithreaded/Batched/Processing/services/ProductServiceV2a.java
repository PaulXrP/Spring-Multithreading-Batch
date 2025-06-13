package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.tomcat.util.threads.VirtualThreadExecutor;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.transaction.support.TransactionTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2a {

    private final ProductRepository productRepository;
    private final TransactionTemplate transactionTemplate; // Injected for programmatic transaction management

    // Use a virtual thread per task executor for better scalability with I/O bound tasks
    private final ExecutorService executorService = Executors.newVirtualThreadPerTaskExecutor();

    private final static int BATCH_SIZE = 2000; //Increased batch size for better performance

    /*
     * Finds all product IDs using a projection query for efficiency.
     * This avoids loading full Product entities into memory.
     */

    public List<Long> findAllProductIds() {
        return productRepository.findAllProductIds();
    }

    /**
     * Resets product records using a single, efficient bulk UPDATE query.
     * This is vastly more performant than fetching and saving each entity individually.
     *
     * @return A success message with the count of updated records.
     */

    @Transactional
    public String resetRecordsWithBulkUpdate() {
        log.info("Starting bulk reset of product records....");
        int updatedCount = productRepository.resetAllProducts();
        log.info("Successfully reset {} product records", updatedCount);
        return "Bulk data reset complete! " + updatedCount + " records updated.";
    }

    /*
     * Applies discount logic to all products in parallel batches.
     * It orchestrates fetching IDs, splitting them into chunks, and processing them concurrently.
     *
     * @return A success message.
     */

    public String applyDiscountsInParallel() {
        List<Long> allProductIds = findAllProductIds();
        if(allProductIds.isEmpty()) {
            return "No products found to process..";
        }

        List<List<Long>> batches = splitIntoBatches(allProductIds, BATCH_SIZE);
        log.info("Processing {} IDS in {} batches", allProductIds.size(), batches);

//        List<CompletableFuture<Void>> futures = batches.stream()
//                .map(batch -> CompletableFuture.runAsync(() -> processProductBatch(batch), executorService))
//                .collect(Collectors.toList());
//
//        // Wait for all asynchronous tasks to complete
//        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        // 1. A list to hold all your asynchronous operations
        List<CompletableFuture<Void>> futures = batches.stream()
                // 2. Take the stream of batches and for each one...
                .map(batch ->
                        // 3. Create an asynchronous task
                        CompletableFuture.runAsync(
                                // 4. This is the work to be done: process one batch
                                () -> processProductBatch(batch),
                                // 5. This is WHERE the work will be done
                                executorService
                        )
                )
                // 6. Collect all the CompletableFuture objects into a list
                .collect(Collectors.toList());

// 7. Create a new CompletableFuture that completes only when ALL others are done
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0]))
                // 8. Block the main thread here and wait for the 'allOf' future to complete
                .join();

        return "Discount processing completed for all batches successfully!";

    }

    /**
     * Splits a list of IDs into smaller batches.
     */
    private List<List<Long>> splitIntoBatches(List<Long> allProductIds, int batchSize) {
         List<List<Long>> batches = new ArrayList<>();

         for(int i = 0; i < allProductIds.size(); i += batchSize) {
             batches.add(allProductIds.subList(i, Math.min(i + batchSize, allProductIds.size())));
         }
         return batches;
    }

    /**
     * Processes a single batch of products. Each batch is handled in its own transaction.
     * This method fetches entities, applies business logic, and saves them back.
     * Using TransactionTemplate gives us fine-grained control over transaction boundaries per thread.
     *
     * @param batchProductIds The list of product IDs for the batch.
     */
    private void processProductBatch(List<Long> batchProductIds) {
        transactionTemplate.execute(status -> {
            try {
                log.info("Processing batch of size {} on thread {}", batchProductIds.size(), Thread.currentThread().getName());

                // 1. Fetch all entities for the batch in a single query
                List<Product> products = productRepository.findAllById(batchProductIds);

                // 2. Apply business logic to each entity
                products.forEach(this::updateDiscountedPrice);

                // 3. Save the updated entities. With batching properties enabled, this will be efficient.
                productRepository.saveAll(products);
            } catch (Exception e) {
                log.error("Error processing batch on thread {}: {}", Thread.currentThread().getName(), e.getMessage());
                status.setRollbackOnly(); //Mark transaction for rollback on error
            }
            return null; // Return value for the TransactionCallback
        });
    }

    /**
     * The core business logic to update a product's discount information.
     * This logic remains unchanged.
     */
    private void updateDiscountedPrice(Product product) {
        Double price = product.getPrice();
        Double discountPercentage;
//        int discountPercentage = (price >= 2000) ? 10 : (price >= 1000) ? 5 : 0;

        // First check
        if (price >= 2000.00) {
            discountPercentage = 10.0;
        } else if (price >= 1000.00) {
            discountPercentage = 5.0;
        } else {
            discountPercentage = 0.0;
        }

        // Add an 'else' block to handle the reset case
        if (discountPercentage > 0) {
            // Apply the new discount
            double priceAfterDiscount = price - (price * discountPercentage / 100);
            product.setOfferApplied(true);
            product.setDiscountPercentage(discountPercentage);
            product.setPriceAfterDiscount(priceAfterDiscount);
        } else {
            // Reset the product to a non-discounted state
            product.setOfferApplied(false);
            product.setDiscountPercentage(0.0);
            product.setPriceAfterDiscount(price); // Reset the price to the original price
        }
    }

}
