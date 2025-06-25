package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.JobCheckpoint;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.ProductError;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.JobCheckpointRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductErrorRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.support.TransactionTemplate;

import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.function.Consumer;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProductServiceV2aChunkSemaphoreWithCheckPoint2 {

    private final ProductRepository productRepository;
    private final ProductErrorRepository productErrorRepository;
    private final JobCheckpointRepository checkpointRepository;
    private final TransactionTemplate transactionTemplate;
    private final MeterRegistry meterRegistry;

    private static final int BATCH_SIZE = 2000;
    private static final int MAX_PARALLEL_BATCHES = 50;
    private static final int MAX_RETRIES = 3;

    private final Semaphore semaphoreBackpressure = new Semaphore(MAX_PARALLEL_BATCHES);

    private ExecutorService executorService;

    private Counter processedCounter;
    private Counter failedCounter;
    private Timer batchTimer;

    @PostConstruct
    public void init() {
       this.executorService = new ThreadPoolExecutor(
               MAX_PARALLEL_BATCHES,
               MAX_PARALLEL_BATCHES,
               0L,
               TimeUnit.MILLISECONDS,
               new LinkedBlockingDeque<>(MAX_PARALLEL_BATCHES * 2),
               r -> {
                   Thread t = new Thread(r);
                   t.setName("product-batch-worker-" + t.getId());
                   return t;
               },
               new ThreadPoolExecutor.CallerRunsPolicy()
       );

       this.processedCounter = Counter.builder("products.processed.count").tag("status", "success").register(meterRegistry);
       this.failedCounter = Counter.builder("products.failed.count").tag("status", "failed").register(meterRegistry);
       this.batchTimer = meterRegistry.timer("products.batch.duration");
    }

    public String applyDiscountsInParallelWithPaging() {
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        streamProductsFrom(batch -> futures.add(processChunkAsync(batch)));
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        log.info("Discount processing completed successfully!");
        return "Discount processing completed successfully!";
    }


    private void streamProductsFrom(Consumer<List<Product>> processor) {
        int lastProcessedId = getLastCheckpoint();

        while (true) {
            List<Product> batch = productRepository.findTop2000ByIdGreaterThanOrderByIdAsc(lastProcessedId);
            if(batch.isEmpty()) break;

            List<Product> batchToProcess = new ArrayList<>(batch);
            processor.accept(batchToProcess);
            lastProcessedId = Math.toIntExact(batchToProcess.get(batchToProcess.size() - 1).getId());
            saveCheckpoint(lastProcessedId);
        }
    }

    private CompletableFuture<Void> processChunkAsync(List<Product> batchToProcess) {

          try {
              semaphoreBackpressure.acquire();
              return CompletableFuture.runAsync(() -> {
                  try {
                      processProductBatchWithRetries(batchToProcess);
                  } finally {
                      semaphoreBackpressure.release();
                  }
              }, executorService);
          } catch (InterruptedException e) {
              Thread.currentThread().interrupt();
              log.error("Semaphore acquisition interrupted", e);
              return CompletableFuture.completedFuture(null);
          }
    }

    private void processProductBatchWithRetries(List<Product> batch) {
        int attempt = 0;
        boolean success = false;

        while (!success && attempt < MAX_RETRIES) {
            try {
                processProductBatch(batch);
                success = true;
            } catch (Exception e){
                attempt++;
                log.warn("Retry {}/{} failed: {}", attempt, MAX_RETRIES, e.getMessage());
                if(attempt == MAX_RETRIES) {
                    failedCounter.increment(batch.size());
                    log.error("Batch permanently failed after {} attempts", MAX_RETRIES, e);
                    batch.forEach(product -> productErrorRepository.save(
                            new ProductError(null, product.getId(), product.getPrice(), "Failed after retries")
                    ));
                } else {
                    try {
                        Thread.sleep(1000L * attempt);
                    } catch (InterruptedException ex) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    private void processProductBatch(List<Product> products) {
        Timer.Sample sample = Timer.start(meterRegistry);
        transactionTemplate.execute(status -> {
            try {
                Instant start = Instant.now();
                log.info("Processing batch of size {} on thread {}", products.size(), Thread.currentThread().getName());

                products.forEach(this::updateDiscountedPrice);
                productRepository.saveAll(products);
                processedCounter.increment(products.size());

                Instant end = Instant.now();
                log.info("Finished batch in {} ms", Duration.between(start, end).toMillis());

            } catch (Exception e) {
                status.setRollbackOnly();
                throw new RuntimeException("Transaction error: " + e.getMessage(), e);
            }
            return null;
        });
        sample.stop(batchTimer);
    }

    private void updateDiscountedPrice(Product product) {
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
            double priceAfterDiscount = price - (price * discountPercentage / 100);
            product.setOfferApplied(true);
            product.setDiscountPercentage(discountPercentage);
            product.setPriceAfterDiscount(priceAfterDiscount);
        } else {
            product.setOfferApplied(false);
            product.setDiscountPercentage(0.0);
            product.setPriceAfterDiscount(price);
        }

        product.setPostProcessed(true);
    }

    private void saveCheckpoint(int lastProcessedId) {
        checkpointRepository.save(new JobCheckpoint("product-discount-checkpoint", lastProcessedId));
        log.info("Checkpoint updated to ID {}", lastProcessedId);
    }

    private int getLastCheckpoint() {
        return checkpointRepository.findById("product-discount-checkpoint")
                .map(JobCheckpoint::getLastProcessedPage)
                .orElse(0);
    }
}
