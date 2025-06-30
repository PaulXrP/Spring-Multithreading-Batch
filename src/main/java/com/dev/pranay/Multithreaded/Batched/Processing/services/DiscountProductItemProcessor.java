package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.item.ItemProcessor;
import org.springframework.stereotype.Component;

/**
 * The ItemProcessor is responsible for the core business logic.
 * It receives one Product from the reader, applies the discount logic,
 * and returns the modified product to be passed to the writer.
 */


@Component
@Slf4j
public class DiscountProductItemProcessor implements ItemProcessor<Product, Product> {
    @Override
    public Product process(Product product) throws Exception {
        // The business logic is identical to your previous implementation.
        // It's now cleanly isolated in this processor component.
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
        // This flag is crucial for idempotency and ensuring the job can be restarted safely.
        // The reader's query (WHERE p.postProcessed = false) depends on this.
        product.setPostProcessed(true);

        log.trace("Processing product ID: {}", product.getId());
        return product;
    }
}
