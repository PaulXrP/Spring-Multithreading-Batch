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
//    @Transactional
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

/*
| Layer                  | Purpose                                                     |
| ---------------------- | ----------------------------------------------------------- |
| `application.yml`      | Enables batching feature inside Hibernate + JDBC            |
| `ProductBatchInserter` | Tells Hibernate **when** to execute the batch (flush/clear) |

| Scenario                                                | What Happens                                                           |
| ------------------------------------------------------- | ---------------------------------------------------------------------- |
| Config present, no `flush()`/`clear()`                  | Hibernate keeps queuing entities → memory bloat, late insert           |
| Code uses `flush()`/`clear()`, but batching not enabled | Hibernate sends **individual** insert SQLs on each flush — no batching |
| ✅ Both present                                          | 🔥 True batching — bulk insert, fewer round-trips, optimal memory      |

 */

/*

Hibernate Batching and JDBC Batching

The concept of "batching" you mentioned ("Enables sending multiple insert/update statements in one JDBC round trip") directly relates to JDBC's capability to execute multiple statements in a single network call to the database.

JDBC Batching:

JDBC itself provides a feature called "batch processing." Instead of sending each INSERT, UPDATE, or DELETE statement individually to the database, you can:

Add multiple SQL statements to a "batch" using addBatch() on Statement or PreparedStatement objects.
Execute the entire batch with a single call to executeBatch().
This significantly reduces network overhead, especially when inserting or updating a large number of records, leading to substantial performance improvements.

How Hibernate uses JDBC Batching:

Hibernate leverages JDBC batching to optimize its database interactions. When you save or update multiple objects in a Hibernate session, if batching is enabled and configured correctly, Hibernate will:

Collect the individual INSERT or UPDATE statements that result from your object operations.
Add these statements to a JDBC batch internally.
Execute the entire batch in a single JDBC call to the database.
This means that even though you're working at the object level with Hibernate, it's intelligently using the underlying JDBC batching mechanism to perform those operations efficiently. You typically configure batching in Hibernate through properties like hibernate.jdbc.batch_size.
 */
