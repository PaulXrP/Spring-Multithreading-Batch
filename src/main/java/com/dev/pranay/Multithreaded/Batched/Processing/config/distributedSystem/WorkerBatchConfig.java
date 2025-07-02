package com.dev.pranay.Multithreaded.Batched.Processing.config.distributedSystem;

import com.dev.pranay.Multithreaded.Batched.Processing.entities.Product;
import com.dev.pranay.Multithreaded.Batched.Processing.services.DiscountProductItemProcessor;
import jakarta.persistence.EntityManagerFactory;
import lombok.RequiredArgsConstructor;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.batch.integration.chunk.RemoteChunkingWorkerBuilder;
import org.springframework.batch.integration.config.annotation.EnableBatchIntegration;
import org.springframework.batch.item.ItemWriter;
import org.springframework.batch.item.database.JpaItemWriter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.PollableChannel;
import org.springframework.transaction.PlatformTransactionManager;

/**
 * This configuration defines the "Worker" nodes in a Remote Chunking setup.
 * Workers listen for messages (chunks of data) from a queue, process them,
 * and save the results to the database.
 */

@Configuration
@EnableBatchIntegration //Absolutely essential for enabling the worker-side components
@RequiredArgsConstructor
public class WorkerBatchConfig {

    private final RemoteChunkingWorkerBuilder<Product, Product> workerBuilder;
    private final DiscountProductItemProcessor productItemProcessor;
    private final EntityManagerFactory entityManagerFactory;
    private final PlatformTransactionManager transactionManager;

//    //Injecting the secure message converter directly from AmqpConfig
    private final MessageConverter messageConverter;
    private final ConnectionFactory connectionFactory;

    // Inject our custom, secure listener container factory from AmqpConfig
//    private final SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory;

    // Inject the new outbound reply channel
    @Qualifier("outboundReplies")
    private final DirectChannel outboundReplies;

    /**
     * This is the in-memory channel that the worker will poll for new chunks.
     * The AMQP inbound adapter will place messages here.
     * @return A pollable QueueChannel.
//     */
//    @Bean
//    public PollableChannel workerInputChannel() {
//        return new QueueChannel();
//    }

    /**
     * This is the in-memory channel that the AMQP listener will send messages to.
     * Because it's a DirectChannel, the processing will happen on the same thread
     * and within the same transaction as the message listener.
//     */
    @Bean
    public DirectChannel workerInputChannel() {
        return new DirectChannel();
    }

    /**
     * Creates the inbound adapter that connects RabbitMQ to our in-memory channel.
     * CRITICAL FIX: We are now creating this adapter manually and explicitly setting
     * our secure message converter on it.
     * @return A fully configured, secure AmqpInboundChannelAdapter.
     */
    @Bean
    public AmqpInboundChannelAdapter workerInboundAdapter() {
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(workerListenerContainer());
        adapter.setOutputChannel(workerInputChannel());
        // This is the key: force the adapter to use our secure converter.
        adapter.setMessageConverter(this.messageConverter);
        return adapter;
    }

    /**
     * Creates the message listener container for the worker's inbound adapter.
     * CRITICAL: We explicitly set the factory to our secure one.
     * @return A configured SimpleMessageListenerContainer.
     */
    @Bean
    public SimpleMessageListenerContainer workerListenerContainer() {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(this.connectionFactory);
        // This ensures it uses the factory with our secure message converter.
//        container.setConnectionFactory(connectionFactory);
        container.setQueueNames(MessagingConfig.CHUNKING_REQUEST_QUEUE);
        // This ensures that the message processing is wrapped in a transaction.
        container.setChannelTransacted(true);
        container.setTransactionManager(this.transactionManager);

        return container;
    }

    /**
     * This IntegrationFlow acts as the AMQP listener. It continuously listens to the
     * RabbitMQ queue and forwards any received messages to our in-memory workerInputChannel.
     * @param connectionFactory The RabbitMQ connection factory.
     * @return The configured IntegrationFlow.
     */
//    @Bean
//    public IntegrationFlow workerAmqpInboundFlow(ConnectionFactory connectionFactory) {
//         return IntegrationFlow
//                 .from(Amqp.inboundAdapter(connectionFactory, MessagingConfig.CHUNKING_REQUEST_QUEUE))
//                 .channel(workerInputChannel())
//                 .get();
//    }

    /**
     * Creates the inbound adapter that connects RabbitMQ to our in-memory channel.
     * We now pass it the fully configured, secure listener container.
     * @return A configured AmqpInboundChannelAdapter.
     */
//    @Bean
//    public AmqpInboundChannelAdapter workerInboundAdapter() {
//        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(workerListenerContainer());
//        adapter.setOutputChannel(workerInputChannel());
//        return adapter;
//    }

    /**
     * This IntegrationFlow defines the worker's main processing logic.
     * It connects the RemoteChunkingWorkerBuilder to the in-memory channel,
     * effectively processing any chunk that the amqpInboundFlow places there.
     * @return The configured worker IntegrationFlow.
     */
//    @Bean
//    public IntegrationFlow workFlow() {
//        return this.workerBuilder
//                .itemProcessor(productItemProcessor)
//                .itemWriter(jpaItemWriter())
//                .inputChannel(workerInputChannel()) // Reads from the in-memory channel
////                .outputChannel(new DirectChannel()) // Replies can be sent here if needed
//                .outputChannel(outboundReplies) //Send replies to the configured outbound channel
//                .build();
//    }

//    /**
//     * This IntegrationFlow defines the worker's main processing logic.
//     * It now uses a more standard DSL format for clarity.
//     */
//    @Bean
//    public IntegrationFlow workFlow() {
//        return IntegrationFlow.from(workerInputChannel()) // Start from the in-memory channel
//                .handle(this.workerBuilder
//                        .itemProcessor(productItemProcessor)
//                        .itemWriter(jpaItemWriter())
//                        .build()) // // Handle messages with the worker handler
//                .channel(outboundReplies)  // Send the result to the outbound channel
//                .get();
//    }


    /**
     * This IntegrationFlow defines the worker's processing logic.
     * It uses the specialized RemoteChunkingWorkerBuilder to construct the entire flow,
     * ensuring the ItemProcessor, ItemWriter, and channels are correctly configured.
     */
    @Bean
    public IntegrationFlow workerFlow() {
        return this.workerBuilder
                .itemProcessor(productItemProcessor)
                .itemWriter(jpaItemWriter())
                .inputChannel(workerInputChannel()) // Reads from the in-memory channel
                .outputChannel(outboundReplies)      // Sends replies to the outbound channel
                .build();
    }

    /**
     * The JpaItemWriter is reused from our single-node job. It's responsible
     * for writing the processed items to the database.
     */
    @Bean
    public JpaItemWriter<Product> jpaItemWriter() {
        JpaItemWriter<Product> jpaItemWriter = new JpaItemWriter<>();
        jpaItemWriter.setEntityManagerFactory(entityManagerFactory);
        return jpaItemWriter;
    }
}
