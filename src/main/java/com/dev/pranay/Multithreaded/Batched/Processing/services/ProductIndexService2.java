package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductProjection;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.elasticsearch.repository.ElasticsearchRepository;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductIndexService2 {

    private final ProductRepository productRepository;

    private final ElasticsearchRepository elasticsearchRepository;

    private final ProductBulkIndexService productBulkIndexService;

    // helper method to map JPA entity to Elasticsearch document
    private ProductDocument mapToDocument(Product product) {
        return new ProductDocument(
                product.getId(),
                product.getName(),
                product.getCategory(),
                product.getPrice(),
                product.isOfferApplied(),
                product.getDiscountPercentage(),
                product.getPriceAfterDiscount()
        );
    }

    /*
     * Indexes a single batch of products to Elasticsearch.
     * This method is called by each concurrent task.
     */

    private void indexProductsInBulk(List<Product> products) {
        if(CollectionUtils.isEmpty(products)) {
            return;
        }

        List<ProductDocument> documents = products.stream()
                .map(this::mapToDocument)
                .collect(Collectors.toList());

        try {
            elasticsearchRepository.saveAll(documents); //Spring data ES uses bulk API
            log.info("Successfully indexed a batch of {} products to Elasticsearch.", documents.size());
        } catch (Exception e) {
            // Log error, potentially add to a retry queue or count failures
            log.error("Error indexing a batch of {} products to Elasticsearch: {}", documents.size(), e.getMessage(), e);
        }
    }

    /*
     * Fetches all products from the primary database in pages and indexes them
     * into Elasticsearch concurrently.
     */

     public String indexAllProductsConcurrently() {
         final int DB_PAGE_SIZE = 1000; //How many products to fetch from DB at a time
         final int ES_CONCURRENT_WRITERS = 4; // Number of threads to write to Elasticsearch

         log.info("Starting concurrent re-indexing of all products...");

         ExecutorService esExecutor = Executors.newFixedThreadPool(ES_CONCURRENT_WRITERS);
         List<Future<?>> indexingFutures = new ArrayList<>();

         Page<Product> productPage;
         int pageNum = 0;
         Long totalProductsSubmittedForIndexing = 0L;

         try {
             do {
                 // 1. Fetch a page of products from the primary database
                 productPage = productRepository.findAll(PageRequest.of(pageNum++, DB_PAGE_SIZE));
                 List<Product> productsFromDbChunk = productPage.getContent();

                 if(!productsFromDbChunk.isEmpty()) {
                     log.info("Fetched page {} with {} products from DB. Submitting for ES indexing.", pageNum - 1, productsFromDbChunk.size());
                     totalProductsSubmittedForIndexing += productsFromDbChunk.size();

                     // 2. Create a copy of the list for the async task
                     List<Product> chunkToProcess = new ArrayList<>(productsFromDbChunk);

                     // 3. Submit the indexing of this chunk to the ExecutorService
                     Future<?> future = esExecutor.submit(() -> {
                         Thread.currentThread().setName("es-indexer-" + Thread.currentThread().getId());
                         indexProductsInBulk(chunkToProcess);
                     });
                     indexingFutures.add(future);

                     // Optional: Simple backpressure - if too many tasks are queued, wait a bit
                     // This is a very basic form; proper backpressure might use Semaphore or inspect queue size
                     if(indexingFutures.size() > DB_PAGE_SIZE * 2) { // e.g., if queue depth > 2*threads
                         log.warn("Indexing queue is growing large ({} tasks). Checking completed tasks to free up space...", indexingFutures.size());
                         // Attempt to clear some completed futures to avoid OOM if DB reading is much faster than ES indexing
                         indexingFutures.removeIf(f -> {
                             if(f.isDone()) {
                                 try {
                                     f.get(); // To catch and log any exceptions from completed tasks
                                 } catch (InterruptedException | ExecutionException e) {
                                     log.error("Error from a completed ES indexing task: {}", e.getMessage(), e.getCause());
                                 }
                                 return true;
                             }
                             return false;
                         });
                     }
                 }
             } while (productPage.hasNext());

             log.info("All {} products fetched from DB and submitted to Elasticsearch indexing workers. Waiting for completion...", totalProductsSubmittedForIndexing);

             // 4. Wait for all submitted indexing tasks to complete
             for(Future<?> future : indexingFutures) {
                 try {
                     future.get(); // Wait for task completion and catch any exceptions
                 } catch (InterruptedException | ExecutionException e) {
                     log.error("Error executing an Elasticsearch indexing task: {}", e.getMessage(), e.getCause());
                     // Handle this error - perhaps log failed chunk details for retry
                 }
             }
             log.info("All Elasticsearch indexing tasks completed.");

         } finally {
             // 5. Shutdown the ExecutorService
             log.info("Shutting down Elasticsearch indexing executor...");
             esExecutor.shutdown();
             try {
                 if(!esExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                     log.warn("Elasticsearch executor did not terminate in 60s. Forcing shutdown...");
                     esExecutor.shutdownNow();
                     if(!esExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
                         log.error("Elasticsearch executor did not terminate.");
                     }
                 }
             } catch (InterruptedException e) {
                 log.error("Interrupted while waiting for Elasticsearch executor to terminate.", e);
                 esExecutor.shutdown();
                 Thread.currentThread().interrupt();
             }
             log.info("Elasticsearch indexing executor shut down.");
         }
         return "Finished concurrent re-indexing of all products.";
     }

     //Native Bulk Service + Multithreaded Logic
    public String indexAllProducts() {
         int pageSize = 1000;
         int threadCount = 4;
         ExecutorService executor = Executors.newFixedThreadPool(threadCount);

         List<Future<?>> futures = new ArrayList<>();

         Long totalCount = productRepository.count();
         int totalSize = (int) Math.ceil((double) totalCount / pageSize);

         log.info("Total records {} with total page of {} ", totalCount, totalSize);

         for(int pageNum = 0; pageNum < totalSize; pageNum++) {
             int finalPage = pageNum;

             futures.add(executor.submit(() -> {
                 Page<ProductProjection> page = productRepository.findAllBy(PageRequest.of(finalPage, pageSize));
                 List<ProductDocument> documents = page.getContent().stream()
                         .map(p -> new ProductDocument(
                                 p.getId(), p.getName(), p.getCategory(), p.getPrice(),
                                 p.getIsOfferApplied(), p.getDiscountPercentage(), p.getPriceAfterDiscount())
                         )
                         .collect(Collectors.toList());

                 try {
                     productBulkIndexService.indexBatch(documents);
                 } catch (IOException e) {
                     log.error("Bulk index failed for page {} + message {}", finalPage, e.getMessage());
                 }
             }));
         }

         for (Future<?> future : futures) {
             try {
                 future.get();
             } catch (ExecutionException | InterruptedException e) {
                 log.error("Error executing an Elasticsearch indexing task: {}", e.getMessage(), e.getCause());
             }
         }

         executor.shutdown();
         try {
             executor.awaitTermination(60, TimeUnit.SECONDS);
         } catch (InterruptedException e) {
             Thread.currentThread().interrupt();
         }

         return "Finished indexing all products.";
    }
}
