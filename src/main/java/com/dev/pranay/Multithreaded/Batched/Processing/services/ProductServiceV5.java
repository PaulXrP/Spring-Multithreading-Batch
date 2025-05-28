package com.dev.pranay.Multithreaded.Batched.Processing.services;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProductServiceV5 {

    private final ProductBatchAsyncService productBatchAsyncService;


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
}
