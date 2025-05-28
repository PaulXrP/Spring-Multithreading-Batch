package com.dev.pranay.Multithreaded.Batched.Processing.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;

@Configuration
@EnableAsync
public class AsyncConfig {

    @Bean(name = "myTaskExecutor")
    public Executor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(5); // adjust based on CPU
        executor.setMaxPoolSize(10); // upper limit
        executor.setQueueCapacity(100); // queued tasks if all threads are busy
        executor.setThreadNamePrefix("async-product-batch-");
        executor.initialize();
        return executor;
    }
}
