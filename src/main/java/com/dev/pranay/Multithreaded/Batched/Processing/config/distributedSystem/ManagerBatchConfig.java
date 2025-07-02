package com.dev.pranay.Multithreaded.Batched.Processing.config.distributedSystem;

import com.dev.pranay.Multithreaded.Batched.Processing.batch.readers.SynchronizedKeysetItemReader;
import com.dev.pranay.Multithreaded.Batched.Processing.repositories.ProductRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.batch.core.Job;
import org.springframework.batch.core.Step;
import org.springframework.batch.core.configuration.annotation.StepScope;
import org.springframework.batch.core.job.builder.JobBuilder;
import org.springframework.batch.core.launch.support.RunIdIncrementer;
import org.springframework.batch.core.repository.JobRepository;
import org.springframework.batch.core.step.builder.StepBuilder;
import org.springframework.batch.integration.chunk.ChunkMessageChannelItemWriter;
import org.springframework.batch.integration.chunk.RemoteChunkingManagerStepBuilderFactory;
import org.springframework.batch.item.ItemReader;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.core.MessagingTemplate;
import org.springframework.messaging.PollableChannel;
import org.springframework.transaction.PlatformTransactionManager;

@Configuration
@RequiredArgsConstructor
public class ManagerBatchConfig {

//    private final RemoteChunkingManagerStepBuilderFactory managerStepBuilderFactory;
    private final JobRepository jobRepository;
    private final PlatformTransactionManager transactionManager;
    private final ProductRepository productRepository;

    // Injecting the AmqpTemplate that has our secure message converter
//    private final AmqpTemplate rabbitTemplate;

    // Use @Qualifier to specify which beans to inject by name, resolving ambiguity.
    @Qualifier("outboundRequests")
    private final DirectChannel outboundRequests; // From MessagingConfig

    @Qualifier("inboundReplies")
    private final PollableChannel inboundReplies; // From MessagingConfig


    @Value("${batch.processing.chunkSize:1000}")
    private int chunkSize;

    /**
     * Defines the Manager Job.
     */
    @Bean
    public Job remoteChunkingManagerJob() {
        return new JobBuilder("remoteChunkingManagerJob", jobRepository)
                .incrementer(new RunIdIncrementer())
                .start(managerStep())
                .build();
    }

    /**
     * Defines the Manager Step. This step reads data and uses a special writer
     * to send chunks to the messaging queue.
     */
//    @Bean
//    public Step managerStep() {
//        return this.managerStepBuilderFactory.get("managerStep")
//                .chunk(chunkSize)
//                .reader(managerKeysetItemReader())
//                .outputChannel(outboundRequests) // Send chunks to this channel
//                .inputChannel(inboundReplies) // Listen for replies on this channel
//                .build();
//    }


    /**
     * Defines the Manager Step. This step now explicitly uses a custom-configured
     * ChunkMessageChannelItemWriter to ensure the correct AmqpTemplate is used.
     */
    @Bean
    public Step managerStep() {
        return new StepBuilder("managerStep", jobRepository)
                .chunk(chunkSize,transactionManager)
                .reader(managerKeysetItemReader())
                .writer(chunkMessageChannelItemWriter()) // Using our explicit writer bean
                .build();

    }

    /**
     * Creates a MessagingTemplate that will be used by the writer.
     * This template is configured to send messages to our outboundRequests channel by default.
     */
    @Bean
    public MessagingTemplate messagingTemplate() {
        MessagingTemplate template = new MessagingTemplate();
        template.setDefaultChannel(this.outboundRequests);
        return template;
    }

    /**
     * CRITICAL FIX: Manually create the ItemWriter and configure it.
     * This ensures it uses the correct AmqpTemplate (with our secure converter)
     * and sends messages to the correct channel.
     */
    @Bean
    @StepScope
    public ChunkMessageChannelItemWriter chunkMessageChannelItemWriter() {
        ChunkMessageChannelItemWriter itemWriter = new ChunkMessageChannelItemWriter();
        itemWriter.setMessagingOperations(messagingTemplate());
        itemWriter.setReplyChannel(this.inboundReplies);
        return itemWriter;
    }

    /**
     * Reuses our robust, thread-safe keyset reader from the previous implementation.
     * It's defined with @StepScope to ensure a new instance is created for each step execution.
     */
    @Bean("managerKeysetItemReader") //Explicitly naming the bean
    @StepScope
    public SynchronizedKeysetItemReader managerKeysetItemReader() {
        return new SynchronizedKeysetItemReader(productRepository, chunkSize);
    }

}
