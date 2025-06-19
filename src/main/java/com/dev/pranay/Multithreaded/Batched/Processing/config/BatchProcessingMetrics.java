package com.dev.pranay.Multithreaded.Batched.Processing.config;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.springframework.stereotype.Component;

import java.time.Duration;

/**
 * A centralized component for managing and exposing all custom metrics related
 * to the batch processing job, using Micrometer.
 */


@Component
public class BatchProcessingMetrics {

    // Counters to track the number of batches processed and failed
    private final Counter batchesProcessedCounter;
    private final Counter batchesFailedCounter;

    // Timer to measure the duration of batch processing attempts
    private final Timer batchProcessingTimer;

    // The MeterRegistry used to create meters and start timers
    private final MeterRegistry meterRegistry;

    public BatchProcessingMetrics(MeterRegistry meterRegistry) {

        this.meterRegistry = meterRegistry;

        // Initialize the counter for successfully processed batches
        this.batchesProcessedCounter = Counter.builder("batch.processing.success.total")
                .description("Total number of batches successfully processed.")
                .tag("status", "success")
                .register(meterRegistry);

        // Initialize the counter for permanently failed batches (after all retries)
        this.batchesFailedCounter = Counter.builder("batch.processing.failed.total")
                .description("Total number of batches that failed permanently.")
                .tag("status", "failed")
                .register(meterRegistry);

        // Initialize the timer to record processing duration
        this.batchProcessingTimer = Timer.builder("batch.processing.duration")
                .description("Time taken to process a single batch.")
                .publishPercentiles(0.5, 0.95, 0.99) //P50, P95, P99 latencies
                .distributionStatisticBufferLength(1024)
                .serviceLevelObjectives(Duration.ofMillis(500), Duration.ofSeconds(1), Duration.ofSeconds(5))
                .register(meterRegistry);
    }

    public void incrementBatchesProcessed() {
        this.batchesProcessedCounter.increment();
    }

    public void incrementBatchesFailed() {
        this.batchesFailedCounter.increment();
    }

    public Timer.Sample startTimer() {
        return Timer.start(this.meterRegistry);
    }

    public void stopTimer(Timer.Sample sample) {
       sample.stop(this.batchProcessingTimer);
    }
}

/**
 * The Bean is Created and Injected: Because met the conditions (the dependency is present,
 * and i didn't create my own MeterRegistry bean), the auto-configuration process runs.
 * It creates a fully configured PrometheusMeterRegistry bean and places it into the
 * Spring Application Context (the collection of all managed objects).
 *
 * Finally, when our BatchProcessingMetrics class's constructor asks for a MeterRegistry,
 * Spring's dependency injection looks in the context, finds the PrometheusMeterRegistry bean
 * that was just auto-configured, and hands it over.
 *
 * So, in short: We provide the dependency, and Spring Boot's auto-configuration automatically
 * creates and configures the bean for us.
 * It's the core "convention over configuration" principle that makes Spring Boot so powerful.
 */
