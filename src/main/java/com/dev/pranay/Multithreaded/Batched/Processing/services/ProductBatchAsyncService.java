package com.dev.pranay.Multithreaded.Batched.Processing.services;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import jakarta.persistence.EntityManager;
import lombok.RequiredArgsConstructor;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
public class ProductBatchAsyncService {

    private final EntityManager entityManager;

    private final ProductParser productParser;

    /*
     Think of @Async as:
        A Spring-managed abstraction over executor-based concurrency,
        with the added benefit of context awareness, lifecycle safety, and
        tight integration into your application layer.
     */

    @Async
    @Transactional
    public CompletableFuture<Void> persistChunk(List<Product> products) {
        for (int i = 0; i < products.size(); i++) {
            entityManager.persist(products.get(i));
            if (i % 100 == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
        return CompletableFuture.completedFuture(null);
    }

    @Async("myTaskExecutor")
    @Transactional
    public CompletableFuture<Void> persistChunk2(List<String> csvLines) {
        List<Product> products = productParser.parseProduct(csvLines);
        for (int i = 0; i < products.size(); i++) {
            entityManager.persist(products.get(i));
            if (i % 100 == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}





    /*
    So, we don't pass a Runnable anymore, because:

    Spring AOP + @Async wraps our persistChunk2(...) method into an async task
    (a Runnable or Callable) automatically.
    That method is submitted to the thread pool (Executor) internally by Spring,
    based on your @Async("myTaskExecutor") annotation.
    The method parameters (List<String> csvLines) are just POJOs — that’s all Spring
    needs to handle the job.

    So:

    futures.add(asyncService.persistChunkAsync(chunkToProcess));
    is functionally equivalent to:

    futures.add(CompletableFuture.runAsync(() -> {
        // custom logic
    }, executor));
    but cleaner, more declarative, and better integrated with Spring features like:

    @Transactional
    Dependency injection
    Exception handling
    Thread pool reuse
     */

