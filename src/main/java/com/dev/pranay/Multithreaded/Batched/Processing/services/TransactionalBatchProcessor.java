package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class TransactionalBatchProcessor {

    private final ProductRepository productRepository;

    /**
     * Processes and saves a batch of products within a new, independent transaction.
     * <p>
     * The {@code Propagation.REQUIRES_NEW} ensures that this method ALWAYS runs in a new transaction.
     * This is critical for error isolation: a failure in this batch will cause its specific
     * transaction to roll back without affecting the main processing loop or any other batches.
     *
     * @param batch The list of products to process.
     */

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void processBatch(List<Product> batch) {
        log.debug("Processing batch of size {} in new transaction on thread {}", batch.size(), Thread.currentThread().getName());

        for (Product product : batch) {
            updateDiscountAndMarkProcessed(product);
        }

        // The saveAll call will be optimized by Hibernate's JDBC batching settings
        // (e.g., spring.jpa.properties.hibernate.jdbc.batch_size) into a single batch insert/update.
        productRepository.saveAll(batch);
    }

    /**
     * The core business logic. It applies discount rules and, crucially,
     * marks the product as processed to ensure the job is idempotent and can be safely restarted.
     *
     * @param product The product to update.
     */
    private void updateDiscountAndMarkProcessed(Product product) {
        double price = product.getPrice();
        double discountPercentage;

        if (price >= 2000.00) {
            discountPercentage = 10.0;
        } else if (price >= 1000.00) {
            discountPercentage = 5.0;
        } else {
            discountPercentage = 0.0;
        }

        if (discountPercentage > 0) {
            product.setOfferApplied(true);
            product.setDiscountPercentage(discountPercentage);
            product.setPriceAfterDiscount(price - (price * discountPercentage / 100));
        } else {
            product.setOfferApplied(false);
            product.setDiscountPercentage(0.0);
            product.setPriceAfterDiscount(price);
        }

        // This is the critical flag that makes the job restartable and prevents
        // processing the same record twice.
        product.setPostProcessed(true);
    }
}
