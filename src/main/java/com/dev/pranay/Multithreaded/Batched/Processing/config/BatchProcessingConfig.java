package com.dev.pranay.Multithreaded.Batched.Processing.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class BatchProcessingConfig {

    @Bean(name = "batchExecutorService")
    public ExecutorService batchExecutorService() {
        // Using virtual threads is ideal for I/O-bound tasks like our database operations,
        // as it allows for high concurrency without consuming many OS threads.
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}
