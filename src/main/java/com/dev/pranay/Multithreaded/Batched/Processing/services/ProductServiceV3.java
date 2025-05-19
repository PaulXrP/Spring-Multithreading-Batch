package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.EntityTransaction;
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
import java.util.concurrent.*;

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

    private static final int BATCH_SIZE = 2000;

    private final EntityManager entityManager;

    private final EntityManagerFactory emf;

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

    public String saveProductFromCsvInBatchMultithreaded(String filePath) {

        List<String> allLines = new ArrayList<>();
        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            while ((line = br.readLine()) != null) {
                allLines.add(line);
            }
        } catch (IOException e) {
            log.error("Error reading CSV file: {}", e.getMessage());
            return "Failed";
        }

        int numOfThread = 4;
        int chunkSize = allLines.size() / numOfThread;
        ExecutorService executor = Executors.newFixedThreadPool(numOfThread);

        List<Future<?>> futures = new ArrayList<>();

        for (int i = 0; i < numOfThread; i++) {
            int start = i * chunkSize;
            int end = (i == numOfThread - 1) ? allLines.size() : (i + 1) * chunkSize;
            List<String> chunks = allLines.subList(start, end);


//            Runnable task = () -> {
//                EntityManager em = emf.createEntityManager();
//                em.getTransaction().begin();
//                new ProductBatchInserter(chunks, em).run();
//                em.getTransaction().commit();
//                em.close();
//            };

            Runnable task = () -> {
                EntityManager em = emf.createEntityManager();
                em.getTransaction().begin(); //  START TRANSACTION

                try {
                    new ProductBatchInserter(chunks, em).run(); // PASS CHUNK
                    em.getTransaction().commit(); //  COMMIT
                } catch (Exception e) {
                    em.getTransaction().rollback(); // ROLLBACK ON FAILURE
                    e.printStackTrace();
                } finally {
                    em.close();
                }
            };

            futures.add(executor.submit(task));
        }

        futures.forEach(f -> {
            try {
                f.get(); //wait for completion
            } catch (Exception e) {
                log.error("Thread failed: {}", e.getMessage());
            }
        });

        executor.shutdown();
        return "Multithreaded batch processing done!";

    /*
    Benefits

        Utilizes multiple CPU cores to speed up CSV parsing and DB insertions.
        Keeps Hibernate batching enabled per-thread.
        Ensures DB writes happen in parallel transactions, improving throughput.
     */
    }

    //Below is a streaming + multithreaded + Hibernate JDBC batching implementation of
    // our CSV loader

    public String loadCsvStreamingInChunks(String filePath) {
        final int CHUNK_SIZE = 2000;
        final int THREAD_POOL_SIZE = 4;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<Future<?>> futures = new ArrayList<>();
        List<String> chunkBuffer = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                if (isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                chunkBuffer.add(line);

                if (chunkBuffer.size() == CHUNK_SIZE) {
                    //clone and clear the buffer
                    List<String> chunkToProcess = new ArrayList<>(chunkBuffer);
                    log.info("Submitting chunk of size {}", chunkToProcess.size());
                    chunkBuffer.clear();

                    futures.add(executor.submit(() -> {
                        EntityManager em = emf.createEntityManager();
                        EntityTransaction transaction = em.getTransaction();

                        try {
                            transaction.begin();
                            new ProductBatchInserter(chunkToProcess, em).run();
                            transaction.commit();
                        } catch (Exception e) {
                            if (transaction.isActive()) {
                                transaction.rollback();
                            }
                            e.printStackTrace();
                        } finally {
                            em.close();
                        }
                    }));
                }
            }

            // Submit remaining chunk after file is read completely
                if(!chunkBuffer.isEmpty()) {
                    List<String> finalChunk = new ArrayList<>(chunkBuffer);

                    futures.add(executor.submit(() -> {
                        EntityManager em = emf.createEntityManager();
                        EntityTransaction tx = em.getTransaction();

                        try {
                            tx.begin();
                            new ProductBatchInserter(finalChunk, em).run();
                            tx.commit();
                        } catch (Exception e) {
                            if(tx.isActive()) {
                                tx.rollback();
                            }
                            e.printStackTrace();
                        } finally {
                            em.close();
                        }
                    }));
                }

            // Wait for all tasks
           for(Future<?> future : futures) {
               try {
                   future.get();
               } catch (ExecutionException e) {
                   log.error("Thread execution failed: {}", e.getMessage());
               }
           }


            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);

        }  catch (IOException | InterruptedException e) {
            log.error("Exception: {}", e.getMessage());
        }
        return "Streaming + Multithreaded batch processing done!";
    }

    /*
    | Advantage                       | Description                               |
| ------------------------------- | ----------------------------------------- |
| ✅ Lower Memory Use              | Only 1 chunk (e.g., 2000 lines) is in RAM |
| ✅ Scalable to GB-size Files     | Can handle millions of records            |
| ✅ Threaded Processing           | Each chunk goes to a worker thread        |
| ✅ Reuses Existing BatchInserter | Our `Runnable` logic doesn’t change      |

     */
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
