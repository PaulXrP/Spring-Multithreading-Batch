package com.dev.pranay.Multithreaded.Batched.Processing.services;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch.core.BulkRequest;
import co.elastic.clients.elasticsearch.core.BulkResponse;
import co.elastic.clients.elasticsearch.core.bulk.BulkOperation;
import co.elastic.clients.elasticsearch.core.bulk.BulkResponseItem;
import co.elastic.clients.elasticsearch.core.bulk.IndexOperation;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductProjection;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
@Slf4j
public class ProductIndexService3 {

    private final ElasticsearchClient elasticsearchClient;
    private final ProductRepository productRepository;

    private final int pageSize;
    private final int threadCount;
    private final String indexName;

    @Autowired
    public ProductIndexService3(
            ElasticsearchClient elasticsearchClient,
            ProductRepository productRepository,
            @Value("${elasticsearch.bulk.pagesize:1000}") int pageSize,
            @Value("${elasticsearch.bulk.threadcount:4}") int threadCount,
            @Value("${elasticsearch.index.name:products}") String indexName
    ) {
         this.elasticsearchClient = elasticsearchClient;
         this.productRepository = productRepository;
         this.pageSize = pageSize;
         this.threadCount = threadCount;
         this.indexName = indexName;
    }


    private ProductDocument mapProjectionToDocument(ProductProjection p) {
        return new ProductDocument(
                p.getId(),
                p.getName(),
                p.getCategory(),
                p.getPrice(),
                p.getIsOfferApplied(),
                p.getDiscountPercentage(),
                p.getPriceAfterDiscount()
        );
    }

    public String indexAllProductsConcurrently() {
        log.info("Starting concurrent bulk indexing for index '{}' with page size {} and thread {} ",
                indexName, pageSize, threadCount);

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<Integer>> futures = new ArrayList<>();

        long totalCount = productRepository.count();

        if(totalCount == 0) {
            log.info("No products found in the database to index.");
            executor.shutdown(); //shutdown the executor even if no task
            return "No products to index!!";
        }

        int totalPages = (int) Math.ceil((double) totalCount / pageSize);

        log.info("Total records to index: {}, Total pages: {}", totalCount, totalPages);


        for(int pageNum = 0; pageNum<totalPages; pageNum++) {
            int finalPageNum = pageNum; //Effectively final for use in lambda

            // Submit task to fetch data and then index it
            Future<Integer> future = executor.submit(() -> {
                Thread.currentThread().setName("es-indexer-page-" + finalPageNum);
                log.debug("Fetching page {} for indexing.", finalPageNum);

                Page<ProductProjection> page = productRepository.findAllBy(PageRequest.of(finalPageNum, pageSize));
                List<ProductDocument> documents = page.getContent().stream()
                        .map(this::mapProjectionToDocument)
                        .collect(Collectors.toList());

                if(documents.isEmpty()) {
                    log.debug("No documents found on page {}.", finalPageNum);
                    return 0; // No documents to index for this page
                }

                try {
                    log.debug("Attempting to bulk index {} docs from page {}.", documents.size(), finalPageNum);
                    return bulkIndex(documents, finalPageNum); // Pass pageNum for logging
                } catch (Exception e) {
                    log.error("Unexpected error during bulk indexing for page {}: {}", finalPageNum, e.getMessage(), e);
                    throw new RuntimeException("Failed to index page " + finalPageNum + " due to an unexpected error.", e);
                }
            });
            futures.add(future);
        }

        long successfullyIndexedCount = 0;
        int failedPagesCount = 0;

        for(Future<Integer> future : futures) {
            try {
                successfullyIndexedCount += future.get(); //.get() will throw ExecutionException if the task threw an exception
            } catch (ExecutionException e) {
                log.error("Error executing indexing task for a page: {}", e.getCause().getMessage(), e.getCause());
                failedPagesCount++;
            } catch (InterruptedException e) {
                log.error("Indexing thread was interrupted while waiting for a page to complete.", e);
                failedPagesCount++;
                Thread.currentThread().interrupt(); // Preserve interrupt status
            }
        }

        log.info("Finished processing all pages. Attempted to index {} pages.", totalPages);
        log.info("Successfully indexed approximately {} documents.", successfullyIndexedCount);
        if (failedPagesCount > 0) {
            log.warn("{} pages encountered errors during indexing.", failedPagesCount);
        }

        shutdownExecutor(executor);
        log.info("✅ Concurrent indexing process complete for index '{}'.", indexName);

        return "Finished indexing all products.";
    }


    private int bulkIndex(List<ProductDocument> documents, int pageNumForLogging) throws IOException {
        if(CollectionUtils.isEmpty(documents)) return  0;

        List<BulkOperation> bulkOperations = documents.stream().map(
                        doc -> BulkOperation.of(op -> op
                                .index(IndexOperation.of(idx -> idx
                                        .index(this.indexName) // Use configured index name
                                        .id(String.valueOf(doc.getId())) //Ensure ID is a String
                                        .document(doc)
                                ))
                        ))
                .collect(Collectors.toList());

        BulkRequest bulkRequest = BulkRequest.of(b -> b.operations(bulkOperations));
        BulkResponse response;

        try {
            response = elasticsearchClient.bulk(bulkRequest);
        } catch (IOException e) {
            log.error("IOException while sending bulk request for page {}: {}", pageNumForLogging, e.getMessage());
            throw e; // Re-throw to be handled by the calling lambda
        }

        int successfulCountInBatch = 0;
        if(response.errors()) {
            log.warn("Bulk request for page {} completed with errors. Processing {} items.", pageNumForLogging, response.items().size());
            int errorCountInBatch = 0;
            for(BulkResponseItem item : response.items()) {
                if(item.error() != null) {
                    errorCountInBatch++;
                    log.error("Failed to index document ID {} (page {}): Status: {}, Type: {}, Reason: {}",
                            item.id(), pageNumForLogging, item.status(), item.error().type(), item.error().reason());
                } else {
                    successfulCountInBatch++;
                }
            }
            log.warn("Page {}: {} documents failed, {} documents succeeded in this batch.", pageNumForLogging, errorCountInBatch, successfulCountInBatch);
        } else {
            successfulCountInBatch = documents.size();
            log.info("Successfully indexed batch of {} documents from page {} with no errors.", successfulCountInBatch, pageNumForLogging);
        }
        return  successfulCountInBatch;
    }

    private void shutdownExecutor(ExecutorService executor) {
        log.debug("Attempting to shut down executor service...");
        executor.shutdown();

        try {
            if(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                log.warn("Executor service did not terminate in 60 seconds. Forcing shutdown...");
                executor.shutdownNow();
                if(!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    log.error("Executor service did not terminate even after forcing.");
                }
            }
        } catch (InterruptedException e) {
            log.warn("Shutdown of executor service was interrupted. Forcing shutdown now.", e);
            executor.shutdown();
            Thread.currentThread().interrupt();
        }
        log.debug("Executor service shut down.");
    }
}
