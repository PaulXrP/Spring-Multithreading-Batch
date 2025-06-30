package com.dev.pranay.Multithreaded.Batched.Processing.batch.listener;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.services.DeadLetterService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.batch.core.SkipListener;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * A listener that hooks into the skip process of a fault-tolerant step.
 * Its primary purpose is to log skipped items to a Dead Letter Queue (DLQ)
 * for later analysis and reprocessing.
 */

@Component
@Slf4j
@RequiredArgsConstructor
public class ProductSkipListener implements SkipListener<Product, Product> {

    // Assuming you have a DeadLetterService from our previous discussion
    // to persist failures to a database table.

    private final DeadLetterService deadLetterService;

    @Override
    public void onSkipInRead(Throwable t) {
        log.warn("An item was skipped during reading due to: {}", t.getMessage());
    }

    @Override
    public void onSkipInWrite(Product item, Throwable t) {
        log.warn("Item {} was skipped during writing due to: {}", item.getId(), t.getMessage());
        deadLetterService.recordFailures("discountProcessingJob", List.of(item.getId()), t.getMessage());
    }

    @Override
    public void onSkipInProcess(Product item, Throwable t) {
        log.warn("Item {} was skipped during processing due to: {}", item.getId(), t.getMessage());
        deadLetterService.recordFailures("discountProcessingJob", List.of(item.getId()), t.getMessage());
    }
}
