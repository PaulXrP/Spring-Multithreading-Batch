package com.dev.pranay.Multithreaded.Batched.Processing.config;

import com.dev.pranay.Multithreaded.Batched.Processing.batch.listener.BatchMetricsListener;
import com.dev.pranay.Multithreaded.Batched.Processing.batch.listener.JobCompletionNotificationListener;
import com.dev.pranay.Multithreaded.Batched.Processing.batch.listener.ProductSkipListener;
import com.dev.pranay.Multithreaded.Batched.Processing.batch.readers.SynchronizedKeysetItemReader;
import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import com.dev.pranay.Multithreaded.Batched.Processing.services.DiscountProductItemProcessor;
import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.PessimisticLockException;
import lombok.RequiredArgsConstructor;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.item.ItemReader;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.batch.item.database.JpaPagingItemReader;
import org.springframework.batch.item.database.builder.JpaPagingItemReaderBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.task.TaskExecutor;
import org.springframework.dao.OptimisticLockingFailureException;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.transaction.PlatformTransactionManager;

import java.util.concurrent.ThreadPoolExecutor;

//This Spring Batch implementation is a best-in-class, production-grade, single-node solution.


@Configuration
@RequiredArgsConstructor
public class BatchJobConfig {

     private final JobRepository jobRepository;
     private final PlatformTransactionManager transactionManager;
     private final EntityManagerFactory entityManagerFactory;
     private final DiscountProductItemProcessor discountProductItemProcessor;
     private final JobCompletionNotificationListener jobCompletionListener;
     private final ProductSkipListener productSkipListener;
     private final BatchMetricsListener batchMetricsListener;
     private final ProductRepository productRepository;

     @Value("${batch.processing.chunkSize:1000}")
     private int chunkSize;

     @Value("${batch.processing.maxConcurrent:10}")
     private int maxConcurrentThreads;

     @Value("${batch.processing.retryLimit:3}")
     private int retryLimits;

     @Value("${batch.processing.skipLimit:100}")
     private int skipLimit;

    /**
     * Defines the main job for processing product discounts.
     * The job consists of a single, parallelized step.
     */
    @Bean
    public Job discountProcessingJob() {
       return new JobBuilder("discountProcessingJob", jobRepository)
               .incrementer(new RunIdIncrementer()) //Ensures each job instance has a unique ID
               .listener(jobCompletionListener)
               .flow(mainStep())
               .end()
               .build();
    }

    /**
     * Defines the main step for the job. This step is configured to be multi-threaded.
     * It reads products, processes them to apply discounts, and writes them back to the database.
     */
    public Step mainStep() {
        return new StepBuilder("mainStep", jobRepository)
                .<Product, Product>chunk(chunkSize, transactionManager) // Process products in chunks
//                .reader(pagingItemReader())keysetItemReader()
                .reader(keysetItemReader()) //new reader bean
                .processor(discountProductItemProcessor)
                .writer(jpaItemWriter())
                .faultTolerant() // Enable fault tolerance
                .retryLimit(retryLimits) // Number of retries for a failed chunk
                .retry(OptimisticLockingFailureException.class) //Specify exceptions that are retryable
                .retry(PessimisticLockException.class)
                .skipLimit(skipLimit) //Max number of items to skip before failing the step
                .skip(Exception.class) //Skip on any exception after retries are exhausted
                .listener(productSkipListener) // Attach the skip listener for DLQ logging
                .listener(batchMetricsListener) // Listener for performance metrics
                .taskExecutor(taskExecutor()) //Enables multi-threaded step execution
                .build();

    }

    /**
     * Creates our new custom, thread-safe, keyset-based reader.
     * It is defined with @StepScope to ensure a new instance is created for each step execution,
     * which is a best practice for stateful readers.
     */
    @Bean
    @StepScope
    public synchronized SynchronizedKeysetItemReader keysetItemReader() {
        return new SynchronizedKeysetItemReader(productRepository, chunkSize);
    }

    /**
     * Creates a thread-safe JpaPagingItemReader.
     * This reader fetches unprocessed products from the database in pages.
     * Spring Batch automatically handles the paging logic (LIMIT/OFFSET).
     */
    public JpaPagingItemReader<Product> pagingItemReader() {
        return new JpaPagingItemReaderBuilder<Product>()
                .name("JpaPagingItemReaderBuilder")
                .entityManagerFactory(entityManagerFactory)
                .queryString("SELECT p FROM Product p WHERE p.postProcessed = false ORDER BY p.id ASC")
                .pageSize(chunkSize)
                .saveState(false) // Important for multi-threaded readers
                .build();
    }

    /**
     * Creates a JpaItemWriter.
     * This writer will persist the processed Product entities back to the database.
     * It automatically leverages Hibernate's batching capabilities for performance.
     */
    public JpaItemWriter<Product> jpaItemWriter() {
        JpaItemWriter<Product> writer = new JpaItemWriter<>();
        writer.setEntityManagerFactory(entityManagerFactory);
        return writer;
    }

    /**
     * Configures the TaskExecutor (thread pool) for parallel step processing.
     */
    public TaskExecutor taskExecutor() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(maxConcurrentThreads);
        executor.setMaxPoolSize(maxConcurrentThreads);
        executor.setThreadNamePrefix("batch-thread-");
        executor.initialize();
        return executor;
    }
}

/**
 * Spring Batch revolves around:
 *
 * Job: The entire batch process.
 * Step: A phase of the job (you can have one or many steps).
 * ItemReader: Reads records.
 * ItemProcessor: Processes records (like applying discounts).
 * ItemWriter: Writes processed records.
 * JobRepository: Stores metadata (steps completed, statuses, checkpoints).
 *
 *
 * 1. Job Launcher (JobLaunchController): An API endpoint to trigger your job on demand.
 *
 * Job Configuration (BatchJobConfig):
 * The blueprint for your job, defining the reader, writer, and multi-threaded execution.
 *
 * Business Logic (DiscountProductItemProcessor):
 * The isolated, testable component that performs the actual data transformation.
 */

/***
 * Our current ProductServiceV2aChunkSemaphoreWithCheckPointAndDLQ and the Spring Batch job
 * are asynchronous, but they are not event-driven.
 * The Reader, Processor, and Writer are all tightly coupled components working together
 * within a single, monolithic Job process.
 * They are not communicating by sending messages to each other through an external broker.
 */

/**
 * This Spring Batch implementation is a best-in-class, production-grade, single-node solution.
 * However, it has not yet implemented the core distributed concepts from the next Phase design.
 * Phase 3 is not an improvement on the current code; it's a fundamental architectural shift
 * from a single-node application to a multi-node distributed system.
 */
