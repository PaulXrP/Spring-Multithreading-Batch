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

    @Async
    @Transactional
    public CompletableFuture<Void> persistChunk(List<Product> products) {
        for(int i = 0; i<products.size(); i++) {
            entityManager.persist(products.get(i));
            if(i%100 == 0) {
                entityManager.flush();
                entityManager.clear();
            }
        }
        return CompletableFuture.completedFuture(null);
    }
}
