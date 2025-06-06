package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.PersistenceContext;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductService {

    private final ProductRepository productRepository;

    private final List<Product> productBuffer = new ArrayList<>();

    private final JdbcTemplate jdbcTemplate;

    private static final int BATCH_SIZE = 50;

    @PersistenceContext
    private final EntityManager entityManager; // Inject EntityManager

    @Transactional
    public String saveProductFromCsvInBatch(String filePath) { //JPA-based with batching
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

                /*
                Bonus: Don't Use saveAll() for High Performance Batching
                        saveAll() does not flush or clear automatically and
                        retains all entities in the persistence context.
                        That causes Hibernate to:

                        Keep tracking each object → memory spike
                        Emit one INSERT per row → you get 10,000 INSERT log lines
                 */
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

    @Transactional
    public String saveProductFromCsvInBatchViaJdbcTemplate(String filePath) {

        //Optimized JDBC Batch Insert

         List<Object[]> batchArgs = new ArrayList<>();

         try (BufferedReader br = new BufferedReader(new FileReader(filePath))){
                String line;
                boolean firstLine = true;

                while ((line = br.readLine()) != null) {
                    if(firstLine) {
                        firstLine = false; //skip header
                        continue;
                    }

                    String[] fields = line.split(",");
                    if(fields.length == 7) {
                        Object[] values = new Object[] {
                                Long.parseLong(fields[0].trim()), //id
                                fields[1].trim(), //name
                                fields[2].trim(), //category
                                Double.parseDouble(fields[3].trim()), //price
                                Boolean.parseBoolean(fields[4].trim()), //isOfferApplied
                                Double.parseDouble(fields[5].trim()), // discountPercentage
                                Double.parseDouble(fields[6].trim()) //priceAfterDiscount
                        };
                        batchArgs.add(values);
                    }

                    if(batchArgs.size() >= 1000) {
                        executeBatchInsert(batchArgs);
                        batchArgs.clear();
                    }
                }

                //save remaining
             if(!batchArgs.isEmpty()) {
                 executeBatchInsert(batchArgs);
             }

         }  catch (IOException e) {
             log.error("Error reading CSV file: {}", e.getMessage());
             return "Failed to save products!!";
         }
        return "Products from csv saved in batches successfully!!";
    }

    private void executeBatchInsert(List<Object[]> batchArgs) {
          String sql = "INSERT INTO products \n" +
                  "(id, name, category, price, is_offer_applied, discount_percentage, price_after_discount)\n" +
                  "VALUES (?, ?, ?, ?, ?, ?, ?)\n";

//          jdbcTemplate.batchUpdate(sql, batchArgs);

        try {
            long start = System.currentTimeMillis();
            jdbcTemplate.batchUpdate(sql, batchArgs);
            long end = System.currentTimeMillis();
            System.out.println("JdbcTemplate Batch Insert Time: " + (end - start) + " ms for " + batchArgs.size() + " records.");
        } catch (DataAccessException e) {
            log.error("DB Batch Insert failed: {}", e.getMessage());
        }
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

    //Load-modify-save (Object-based)
    public String resetRecords() {
        List<Product> all = productRepository.findAll();

        all.forEach(product -> {
                    product.setOfferApplied(false);
                    product.setPriceAfterDiscount(product.getPrice());
                    product.setDiscountPercentage((double) 0L);
                });


        productRepository.saveAll(all); //single batch save
        return "Data reset to DB!!!";
    }

    //JPQL bulk update
    @Transactional
    public String resetRecordsJPQL() {
        productRepository.resetAllProductFields();
        return "Data reset to DB!!!";
    }

    //Native SQL with H2
    @Transactional
    public String resetAllWithNativeSQL() {
        productRepository.resetAllProductFieldsNative();
        return "Reset done with Native SQL!";
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

        product.setDiscountPercentage((double) discountPercentage);
        product.setPriceAfterDiscount(priceAfterDiscount);
    }

    public String deleteDb() {

        //1 SELECT + N DELETE
        productRepository.deleteAll(); //Loads All Entities into Persistence Context (1st-level cache
        //and then mark for deletion...That's N SQL DELETEs if N records exist.
        return "Deleted DB records successfully!!!";
    }

    @Transactional
    public String deleteDbOptimized() {
        //1 DELETE
       productRepository.deleteAllInBatch(); //Uses a single DELETE statement
        //The delete happens in one atomic SQL statement — pushed straight to the DB.

       entityManager.clear(); // Important! Clear the first-level cache to maintain consistency
        //bcz entity manager had previously loaded same data that cache should be erased now
        //to be in sync with database post deletion...
        return "Deleted DB records successfully using bulk delete!";
    }

    //If table has 100k+ rows and you want to avoid full-table lock,
    // then prefer batched deletions by slice

    @Transactional
    public String deleteByBatch() {
        int batchSize = 1000;
        List<Product> batch;

        do {
            batch = productRepository.findTop1000By();
            if(!batch.isEmpty()) {
                productRepository.deleteAllInBatch(batch);
                entityManager.clear(); //clear persistence context after each batch
            }
        } while (!batch.isEmpty());

        return "DB deleted in batches successfully!!!";
    }


    @Transactional
    public String deleteProductsCheaperThan(double priceThreshold) {

        int deletedProductsBelowPrice = productRepository.deleteProductsBelowPrice(priceThreshold);

        entityManager.clear(); // Crucial! Clear the first-level cache after bulk operation

        return String.format("Deleted %d products with price <= %.2f successfully!", deletedProductsBelowPrice, priceThreshold);

    }

    @org.springframework.transaction.annotation.Transactional
    public String deleteProductsByCategory(String category) {
        int deletedByCategory = productRepository.deleteByCategory(category);

        entityManager.clear();

        return String.format("Deleted %d products in category '%s' successfully!", deletedByCategory, category);

    }
}
