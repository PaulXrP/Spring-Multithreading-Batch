package com.dev.pranay.Multithreaded.Batched.Processing.config.distributedSystem;

import com.rabbitmq.client.AMQP;
import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.rabbit.listener.SimpleMessageListenerContainer;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.dsl.Amqp;
import org.springframework.integration.amqp.dsl.SimpleMessageListenerContainerSpec;
import org.springframework.integration.amqp.inbound.AmqpInboundChannelAdapter;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.channel.QueueChannel;
import org.springframework.integration.dsl.IntegrationFlow;
import org.springframework.messaging.PollableChannel;

/**
 * This configuration class sets up the necessary Spring Integration and AMQP
 * components for communication between the Manager and Worker nodes.
 */

@Configuration
public class MessagingConfig {

    // This is the name of the queue that will be created in RabbitMQ
    public static final String CHUNKING_REQUEST_QUEUE = "chunking.requests";
    public static final String CHUNKING_REPLY_QUEUE = "chunking.replies";

    /**
     * Creates the durable queue in RabbitMQ where the Manager will send work requests.
     */
    @Bean
    public Queue chunkingRequestQueue() {
        return new Queue(CHUNKING_REQUEST_QUEUE, true);
    }

    /**
     * Creates the durable queue in RabbitMQ where Workers will send back replies.
     */
    @Bean
    public Queue chunkingReplyQueue() {
       return new Queue(CHUNKING_REPLY_QUEUE, true);
    }

    /**
     * Explicitly defines the AmqpTemplate bean.
     * This makes the dependency clear and helps resolve configuration issues.
     * @param connectionFactory The auto-configured RabbitMQ connection factory.
     * @return An AmqpTemplate instance.
     */
//    @Bean
//    public AmqpTemplate amqpTemplate(ConnectionFactory connectionFactory) {
//        return new RabbitTemplate(connectionFactory);
//    }

    // --- Channels for the Manager Node ---

    /**
     * This is the outbound channel the Manager's ItemWriter will send messages to.
     * It's a DirectChannel, meaning messages are sent directly to its subscriber (the outbound flow).
     */
    // --- Outbound Flow (Manager -> Worker) ---
    // --- Manager -> Worker Communication ---
    @Bean
    public DirectChannel outboundRequests() {
        return new DirectChannel();
    }

    /**
     * This IntegrationFlow connects the `outboundRequests` channel to RabbitMQ.
     * Any message sent to the channel will be routed to the CHUNKING_REQUEST_QUEUE.
     * @param amqpTemplate The RabbitMQ template for sending messages.
     */
    @Bean
    public IntegrationFlow amqpOutboundFlow(AmqpTemplate amqpTemplate) {
         return IntegrationFlow.from(outboundRequests())
                 .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(CHUNKING_REQUEST_QUEUE))
                 .get();
    }

    // --- Channels for the Worker Node ---

    /**
     * This is the inbound channel the Worker's ItemReader will receive messages from.
     * It's a PollableChannel, meaning the reader can actively poll it for new messages.
     */
    // --- Inbound Flow (Worker -> Manager) ---
    @Bean
    public PollableChannel inboundReplies() {
        return new QueueChannel();
    }


    /**
     * Creates the message listener container for the Manager's reply queue.
     * This component is responsible for connecting to RabbitMQ and receiving messages.
     * @param connectionFactory The auto-configured RabbitMQ connection factory.
     * @return A configured SimpleMessageListenerContainer.
     */
    @Bean
    public SimpleMessageListenerContainer managerReplyListenerContainer(ConnectionFactory connectionFactory) {
        SimpleMessageListenerContainer container = new SimpleMessageListenerContainer(connectionFactory);
        container.setQueueNames(CHUNKING_REPLY_QUEUE);
        return container;
    }

    /**
     * UPDATED: This bean now correctly creates the inbound adapter for the Manager's replies.
     * It uses the listener container and is configured with our secure message converter.
     * @param managerReplyListenerContainer The listener container for the reply queue.
     * @param messageConverter Our custom, secure message converter from AmqpConfig.
     * @return A configured AmqpInboundChannelAdapter.
     */
    @Bean
    public AmqpInboundChannelAdapter amqpInboundAdapter(SimpleMessageListenerContainer managerReplyListenerContainer,
                                                        MessageConverter messageConverter) {
        AmqpInboundChannelAdapter adapter = new AmqpInboundChannelAdapter(managerReplyListenerContainer);

        adapter.setOutputChannel(inboundReplies());
        // This is the critical : forcing the adapter to use our secure converter.
        adapter.setMessageConverter(messageConverter);
        return adapter;
    }
    /**
     * This IntegrationFlow connects RabbitMQ to our application.
     * It listens to the CHUNKING_REPLY_QUEUE and places any received messages
     * onto the `inboundReplies` channel for the Manager to process.
     * @param connectionFactory The RabbitMQ connection factory.
//     */
//    @Bean
//    public IntegrationFlow amqpInboundFlow(ConnectionFactory connectionFactory) {
//        return IntegrationFlow.from(Amqp.inboundAdapter(connectionFactory, CHUNKING_REPLY_QUEUE))
//                .channel(inboundReplies())
//                .get();
//    }

    /**
     * The new approach in the canvas is more explicit and manual.
     * We have replaced that single amqpInboundFlow bean with two more specific beans:
     *
     * managerReplyListenerContainer(): This manually creates the listener container.
     *
     * amqpInboundAdapter(): This manually creates the channel adapter, passes it the container,
     * and, most importantly, allows us to call .setMessageConverter() to fix the security issue.
     */

    // ---  Outbound Flow for Worker Replies ---

    /**
     * This is the channel the worker will send its replies to.
     */
    @Bean
    public DirectChannel outboundReplies() {
        return new DirectChannel();
    }

    /**
     * This new flow connects the worker's reply channel to RabbitMQ,
     * sending the reply messages to the dedicated replies queue.
     */
    @Bean
    public IntegrationFlow amqpReplyOutboundFlow(AmqpTemplate amqpTemplate) {
        return IntegrationFlow.from(outboundReplies())
                .handle(Amqp.outboundAdapter(amqpTemplate).routingKey(MessagingConfig.CHUNKING_REPLY_QUEUE))
                .get();
    }
}
