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
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    private final List<Product> productBuffer = new ArrayList<>();

    private static final int BATCH_SIZE = 50;

    public String saveProductFromCsvInBatch(String filePath) {
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                if (firstLine) {
                    firstLine = false; // Skip header
                    continue;
                }

                String[] fields = line.split(",");

                if (fields.length == 7) {
                    Product product = new Product();
                    product.setId(Long.parseLong(fields[0].trim()));
                    product.setName(fields[1].trim());
                    product.setCategory(fields[2].trim());
                    product.setPrice(Double.parseDouble(fields[3].trim()));
                    product.setOfferApplied(Boolean.parseBoolean(fields[4].trim()));
                    product.setDiscountPercentage(Double.parseDouble(fields[5].trim()));
                    product.setPriceAfterDiscount(Double.parseDouble(fields[6].trim()));
                    productBuffer.add(product);
                }

                if (productBuffer.size() >= BATCH_SIZE) {
                    productRepository.saveAll(productBuffer);
                    productBuffer.clear();
                }
            }

            // Save remaining
            if (!productBuffer.isEmpty()) {
                productRepository.saveAll(productBuffer);
                productBuffer.clear();
            }

        } catch (IOException e) {
            log.error("Error reading CSV file: {}", e.getMessage());
            return "Failed to save products!!";
        }

        return "Products from csv saved in batches successfully!!";
    }

    public List<Long> getAllIds() {
        return productRepository.findAll()
                .stream().map(Product::getId)
                .collect(Collectors.toList());
    }

//    public String resetRecords() {
//        productRepository.findAll()
//                .forEach(product -> {
//                    product.setOfferApplied(false);
//                    product.setPriceAfterDiscount(product.getPrice());
//                    product.setDiscountPercentage(0);
//                    productRepository.save(product);
//                });
//        return "Data reset to DB!!!";
//    }

    public String resetRecords() {
        List<Product> all = productRepository.findAll();

        all.forEach(product -> {
                    product.setOfferApplied(false);
                    product.setPriceAfterDiscount(product.getPrice());
                    product.setDiscountPercentage(0);
                });


        productRepository.saveAll(all); //single batch save
        return "Data reset to DB!!!";
    }

    @Transactional
    public String processProductIds(List<Long> productIds) {

        //using only stream
//        productIds.stream()
//                .forEach(this::fetchUpdateAndPublish);

        //using parallelStream
        productIds.parallelStream()
                .forEach(this::fetchUpdateAndPublish);

        return "Ids processed successfully!!!!";
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

        product.setDiscountPercentage(discountPercentage);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }

    public String deleteDb() {
        productRepository.deleteAll();
        return "Deleted DB records successfully!!!";
    }
}
