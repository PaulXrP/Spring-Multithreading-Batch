package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV3 {

    /*
    Original Problem: No JDBC Batching by Default in Hibernate

    Even though we’re grouping records into batches (saveAll() of 500 or 1000),
    Hibernate’s default behavior is:

    One SQL INSERT per entity
    No JDBC batch optimization, unless told to do so via properties
    So you still get 1000 individual INSERT queries instead of 1 JDBC call inserting 1000 rows.

    Solution: Enable Hibernate JDBC Batching

    Add these to your application.properties or application.yml:
     */

    /*
    Even after configuring Hibernate batching in application.yml,
    Hibernate still won’t batch unless we clear the persistence context.
     */

    /*
    Bonus: JDBC is Still Faster

    Even after enabling Hibernate batching, raw JdbcTemplate.batchUpdate() is still faster
    and more predictable, because:

    No entity lifecycle overhead (no dirty checking, flushing, etc.)
    No proxy handling
    No Hibernate context management
     */

    private ProductRepository productRepository;

    private final List<Product> productBuffer = new ArrayList<>();

    private static final int BATCH_SIZE = 50;

    private final EntityManager entityManager;

    @Transactional
    public String saveProductFromCsvInBatch(String filePath) {

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {

            String line;
            Boolean firstLine = true;

            while ((line = br.readLine()) != null) {
                 if(firstLine) {
                     firstLine = false; //skip header
                     continue;
                 }

                 String[] fields = line.split(",");

                 if(fields.length == 7) {
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

                 if(productBuffer.size() >= BATCH_SIZE) {
                     productBuffer.forEach(entityManager::persist);
                     entityManager.flush(); //write to DB
                     entityManager.clear(); // clear persistence context
                     productBuffer.clear();;
                 }
            }

            if(!productBuffer.isEmpty()) {
                productBuffer.forEach(entityManager::persist);
                entityManager.flush();
                entityManager.clear();
                productBuffer.clear();;
            }
        }  catch (IOException e) {
            log.error("Error reading CSV file: {}", e.getMessage());
            return "Failed to save products!!";
        }
        return "Products from csv saved in batches successfully!!";
    }
}

/*
When you call productRepository.saveAll(productBuffer);, Hibernate:

Adds all entities to the persistence context (i.e., EntityManager’s memory).
Even with batching enabled, if the persistence context isn’t cleared, Hibernate still issues individual INSERTs.

Solution
You need to flush and clear the persistence context manually to activate batching.
 */

/**
 * Don't Use saveAll() for High Performance Batching
 * saveAll() does not flush or clear automatically and retains all entities in the persistence context. That causes Hibernate to:
 *
 * Keep tracking each object → memory spike
 * Emit one INSERT per row → you get 10,000 INSERT log lines
 */
