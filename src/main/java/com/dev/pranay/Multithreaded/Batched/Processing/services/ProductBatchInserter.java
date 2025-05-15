package com.dev.pranay.Multithreaded.Batched.Processing.services;



import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.RequiredArgsConstructor;

import java.util.List;

//Full Multi-threaded Setup
//Step 1: Create a ProductBatchInserter Runnable
// Step 2: Modify ProductServiceV3 with ExecutorService

@RequiredArgsConstructor
public class ProductBatchInserter implements Runnable {

    private final List<String> lines;
    private final EntityManager em;
    private final static int BATCH_SIZE = 2000;


    @Override
    @Transactional
    public void run() {
        try {
            int count = 0;
            for(String line : lines) {
                String[] fields = line.split(",");

                if(fields.length != 7 || fields[0].equals("id"))
                    continue;

                Product product = new Product();
                product.setId(Long.parseLong(fields[0].trim()));
                product.setName(fields[1].trim());
                product.setCategory(fields[2].trim());
                product.setPrice(Double.parseDouble(fields[3].trim()));
                product.setOfferApplied(Boolean.parseBoolean(fields[4].trim()));
                product.setDiscountPercentage(Double.parseDouble(fields[5].trim()));
                product.setPriceAfterDiscount(Double.parseDouble(fields[6].trim()));

                em.persist(product);
                count++;

                if(count % BATCH_SIZE == 0) {
                    em.flush();
                    em.clear();
                }
            }
            em.flush();
            em.clear();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
