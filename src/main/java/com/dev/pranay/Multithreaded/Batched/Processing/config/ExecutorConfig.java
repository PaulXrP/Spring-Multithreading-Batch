package com.dev.pranay.Multithreaded.Batched.Processing.config;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration
public class ExecutorConfig {

    /**
     * Creates a dedicated thread pool for the post-processing job.
     * <p>
     * Configuration Details:
     * - corePoolSize (10): The number of threads to keep in the pool, even if they are idle.
     * - maximumPoolSize (30): The maximum number of threads allowed in the pool.
     * - keepAliveTime (60s): Time an idle thread will wait before being terminated.
     * - workQueue (LinkedBlockingQueue): A queue to hold tasks before they are executed.
     * Sized at 1000 to buffer a reasonable number of batches.
     * - threadFactory: Names the threads for easier debugging and monitoring (e.g., "post-process-0").
     * - rejectedExecutionHandler (CallerRunsPolicy): A backpressure policy. If the pool and queue are full,
     * the thread that submitted the task will run it, effectively slowing down submission.
     *
     * @return A configured ExecutorService bean.
     */

    @Bean(name = "postProcessingExecutor")
    public ExecutorService postProcessingExecutor() {
        return new ThreadPoolExecutor(
                10,
                30,
                60L,
                TimeUnit.SECONDS,
                new LinkedBlockingDeque<>(1000),
                new ThreadFactoryBuilder().setNameFormat("post-process-%d").build(),
                new ThreadPoolExecutor.CallerRunsPolicy()
        );
    }
}
