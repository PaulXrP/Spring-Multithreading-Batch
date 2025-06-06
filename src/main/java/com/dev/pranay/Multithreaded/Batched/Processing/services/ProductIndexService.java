package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductDocument;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductProjection;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductElasticsearchRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
public class ProductIndexService {

    private final ProductRepository productRepository;

    private final ProductElasticsearchRepository productElasticsearchRepository;

    // Method to map JPA entity to Elasticsearch document
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

    public void indexAllProducts() {
        // For very large datasets, process in pages/batches to avoid OutOfMemoryError
        int pageSize = 1000;
        Page<Product> productPage;
        int pageNum = 0;

        do {
            productPage = productRepository.findAll(PageRequest.of(pageNum++, pageSize));
            List<ProductDocument> documents = productPage.getContent().stream()
                    .map(this::mapToDocument)
                    .collect(Collectors.toList());

            if(!documents.isEmpty()) {
                productElasticsearchRepository.saveAll(documents); //Bulk save to ES
                System.out.println("Indexed " + documents.size() + " products from page " + (pageNum-1));
            }
        } while (productPage.hasNext());
        System.out.println("Finished indexing all products.");
    }

    //Refactored Indexing Code (Multithreaded)
    public String indexAllProductsMultithreaded() {
        int pageSize = 1000;
        int threadCount = 4;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        List<Future<?>> futures = new ArrayList<>();

        long totalCount = productRepository.count();
        int totalPages = (int) Math.ceil((double) totalCount /pageSize);

        System.out.println("Total records: " + totalCount + " Total pages: " + totalPages);

        for(int pageNumber = 0; pageNumber<totalPages; pageNumber++) {
            int finalPageNum = pageNumber;

            futures.add(executor.submit(() -> {
                Page<ProductProjection> page = productRepository.findAllBy(PageRequest.of(finalPageNum, pageSize));
                List<ProductDocument> documents = page.getContent().stream()
                        .map(p -> new ProductDocument(
                                p.getId(), p.getName(), p.getCategory(), p.getPrice(),
                                p.getIsOfferApplied(), p.getDiscountPercentage(), p.getPriceAfterDiscount()))
                        .collect(Collectors.toList());

                if(!documents.isEmpty()) {
                    productElasticsearchRepository.saveAll(documents);
                    System.out.println("Indexed " + documents.size() + " documents from page " + finalPageNum);
                }
            }));
        }

        //wait for all tasks to complete
        for (Future<?> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                throw new RuntimeException(e);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        executor.shutdown();
        try {
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
      return "Finished indexing all products.";

        /*
        Hitting the re-indexing endpoint again will re-process all products from our database.
        For products already in Elasticsearch (matched by ID), their data in Elasticsearch
        will be updated to reflect the current state in our database.
        This is generally the desired behavior for a re-indexing operation,
        as it ensures Elasticsearch is synchronized with our source of truth.
         */
    }

    // Index a single product (useful for ongoing sync)
    public void indexProduct(Product product) {
        productElasticsearchRepository.save(mapToDocument(product));
    }

    // Delete a product from index (if it's deleted from DB)
    public void deleteProductFromIndex(Long productId) {
       productElasticsearchRepository.deleteById(productId);
    }
}
