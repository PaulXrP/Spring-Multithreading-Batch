package com.dev.pranay.Multithreaded.Batched.Processing.config.distributedSystem;

import org.springframework.amqp.rabbit.config.SimpleRabbitListenerContainerFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.amqp.support.converter.MessageConverter;
import org.springframework.amqp.support.converter.SimpleMessageConverter;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * Global configuration for AMQP (RabbitMQ) to address deserialization security.
 * This configuration defines a custom MessageConverter that specifies which classes
 * are trusted and safe to be deserialized from incoming messages.
 */

@Configuration
public class AmqpConfig {

    /**
     * Defines our custom MessageConverter with a trusted "allowed list" of classes.
     * This is the core of our security fix.
     */
    @Bean
    public MessageConverter messageConverter() {

        SimpleMessageConverter messageConverter = new SimpleMessageConverter();
        // We provide a list of trusted class name patterns.
        // The '*' is a wildcard.
        messageConverter.setAllowedListPatterns(List.of(
                // Trust Spring Batch Integration classes (ChunkRequest)
                "org.springframework.batch.integration.chunk.*",
                // Trust Spring Batch core context classes (StepExecution, JobExecution, etc.)
                "org.springframework.batch.core.*",
                // Trust Spring Batch item classes (Chunk, ExecutionContext)
                "org.springframework.batch.item.*",
                // Trust our own Product entity
                "com.dev.pranay.Multithreaded.Batched.Processing.entities.*",
                // Trust standard Java utility classes
                "java.util.*",
                // Trust standard Java time classes
                "java.time.*",
                // Trust core Java language classes like Enum, Long, etc.
                "java.lang.*"
                ));
        return messageConverter;
    }

    /**
     * Configures the RabbitTemplate used for SENDING messages.
     * We explicitly set our custom message converter on it.
     * The Manager node's outbound adapter will use this template.
     */
    @Bean
    public RabbitTemplate rabbitTemplate(final ConnectionFactory connectionFactory,
                                         final MessageConverter messageConverter) {
          final RabbitTemplate rabbitTemplate = new RabbitTemplate(connectionFactory);
          rabbitTemplate.setMessageConverter(messageConverter);
          return rabbitTemplate;
    }

    /**
     * Configures the factory used for creating message listeners for RECEIVING messages.
     * We explicitly set our custom message converter on it.
     * The Worker node's inbound adapter will use this factory.
     */
    @Bean
    public SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory(
            final ConnectionFactory connectionFactory, final MessageConverter messageConverter
    ) {
        SimpleRabbitListenerContainerFactory rabbitListenerContainerFactory = new SimpleRabbitListenerContainerFactory();
        rabbitListenerContainerFactory.setConnectionFactory(connectionFactory);
        rabbitListenerContainerFactory.setMessageConverter(messageConverter);
        return rabbitListenerContainerFactory;
    }
}
