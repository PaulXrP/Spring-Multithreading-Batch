package com.dev.pranay.Multithreaded.Batched.Processing.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductServiceV4 {

    private final ProductBatchAsyncService productBatchAsyncService;

    //using Executor + Async Service
    public String loadCsvStreamingInChunks(String filePath) {
        final int CHUNK_SIZE = 2000;
        final int THREAD_POOL_SIZE = 4;

        ExecutorService executor = Executors.newFixedThreadPool(THREAD_POOL_SIZE);
        List<CompletableFuture<Void>> futures = new ArrayList<>();
        List<String> chunkBuffer = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))){
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                if(isFirstLine) {
                    isFirstLine = false;
                    continue;
                }

                chunkBuffer.add(line);

                if(chunkBuffer.size() == CHUNK_SIZE) {
                    List<String> chunkToProcess = new ArrayList<>(chunkBuffer);
                    chunkBuffer.clear();

                    futures.add(CompletableFuture.runAsync(() -> {
                        new ProductBatchInserterForAsync(chunkToProcess, products -> {
                            productBatchAsyncService.persistChunk(products).join(); //important: block inside thread
                        }).run();
                    }, executor));
                }
            }

            if(!chunkBuffer.isEmpty()) {
                List<String> finalChunk = new ArrayList<>(chunkBuffer);
                futures.add(CompletableFuture.runAsync(() -> {
                    new ProductBatchInserterForAsync(finalChunk, products -> {
                        productBatchAsyncService.persistChunk(products).join();
                    }).run();
                }, executor));
            }


            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
            executor.shutdown();
            executor.awaitTermination(1, TimeUnit.HOURS);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }  catch (IOException | InterruptedException e) {
            log.error("Exception: {}", e.getMessage());
            Thread.currentThread().interrupt();
        }

        return "Streaming + Multithreaded batch processing done!";
    }


    // this version is using only Spring’s @Async mechanism for concurrency,
    // without manually creating an ExecutorService
    public String loadCsvStreamingInChunks2(String filePath) {
        final int CHUNK_SIZE = 2000;
        List<String> chunkBuffer = new ArrayList<>();
        List<CompletableFuture<Void>> futures = new ArrayList<>();

        try (BufferedReader br = new BufferedReader(new FileReader(filePath))){
            String line;
            boolean isFirstLine = true;

            while ((line = br.readLine()) != null) {
                if(isFirstLine) {
                    isFirstLine = false; //skip header
                    continue;
                }

                chunkBuffer.add(line);

                if(chunkBuffer.size() == CHUNK_SIZE) {
                    List<String> chunkToProcess = new ArrayList<>(chunkBuffer);
                    chunkBuffer.clear();

                    futures.add(productBatchAsyncService.persistChunk2(chunkToProcess));
                }
            }

            // handle remaining lines
            if(!chunkBuffer.isEmpty()) {
                futures.add(productBatchAsyncService.persistChunk2(new ArrayList<>(chunkBuffer)));
            }

            // wait for all async chunks to complete
            CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).join();
        } catch (IOException e) {
            log.error("Error processing CSV: {}", e.getMessage());
        }
        return "Async batch processing completed!";
    }

    /*
     If loadCsvStreamingInChunks2 and persistChunk2 were in the same service class:
    Then @Async on persistChunk2 would not have worked at all — the method would run synchronously,
    and you'd see no parallelism or background threading.

    Why?
    Because Spring AOP (Aspect-Oriented Programming) only applies @Async,
     @Transactional, and similar annotations when a Spring proxy calls the method.

     */
}
