package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2 {

    private final ProductRepository productRepository;

    private final List<Product> productBuffer = new ArrayList<>();

    private static final int BATCH_SIZE = 50;

    private final ExecutorService executorService = Executors.newFixedThreadPool(5);



//    @Transactional
    public List<Long> getAllIds() {
        return productRepository.findAll()
                .stream().map(Product::getId)
                .collect(Collectors.toList());
    }

    public List<Long> findAllProductIds() {
        return productRepository.findAllProductIds();
    }

    public String resetRecords() {
        productRepository.findAll()
                .forEach(product -> {
                    product.setOfferApplied(false);
                    product.setPriceAfterDiscount(product.getPrice());
                    product.setDiscountPercentage((double) 0);
                    productRepository.save(product);
                });
        return "Data reset to DB!!!";
    }

    public String executeProductIds(List<Long> productIds) {

        List<List<Long>> batches = splitIntoBatches(productIds, 50);

        List<CompletableFuture<Void>> futures = batches
                .stream()
                .map(batch -> CompletableFuture.runAsync(() ->
                        processProductIds(batch), executorService))
                .collect(Collectors.toList());

        //wait for all future to complete
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();

        return "Ids processed successfully via batches!!!!";
    }

    private List<List<Long>> splitIntoBatches(List<Long> productIds, int batchSize) {

        int totalSize = productIds.size();

        int batchNumber = (totalSize + batchSize - 1) / batchSize;
        // batchNumber = (105 + 50 - 1) / 50 = 154 / 50 = 3

        //calculate number of batch

        List<List<Long>> batches = new ArrayList<>();

        for(int i = 0; i < batchNumber; i++) {
            int start = i * batchSize;
            int end = Math.min(totalSize, (i + 1) * batchSize);
            batches.add(productIds.subList(start, end));
        }

        return batches;

    }

//    @Transactional
    private void processProductIds(List<Long> batch) {

        //using only stream
//        productIds.stream()
//                .forEach(this::fetchUpdateAndPublish);

//        //using parallelStream
//        productIds.parallelStream()
//                .forEach(this::fetchUpdateAndPublish);
//
//        return "Ids processed successfully!!!!";

        System.out.println("Processing batch " + batch + " by thread :" + Thread.currentThread().getName());
        batch.forEach(this::fetchUpdateAndPublish);
    }

    private void fetchUpdateAndPublish(Long productId) {

        //fetch product by Id

        Product product = productRepository.findById(productId)
                .orElseThrow(() -> new RuntimeException("Product with given id not found!!!"));

        //update discount properties
        updateDiscountedPrice(product);

        //save to DB
        productRepository.save(product);
    }

    private void updateDiscountedPrice(Product product) {

        double price = product.getPrice();

        int discountPercentage = (price >= 300) ? 10 : (price >= 100? 5 : 0);

        double priceAfterDiscount = price - (price * discountPercentage/100);

        if(discountPercentage > 0) {
            product.setOfferApplied(true);
        }

        product.setDiscountPercentage((double) discountPercentage);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }
}
