package com.example.kafkalab.config;

import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.DelegatingByTypeSerializer;
import org.springframework.kafka.support.serializer.JacksonJsonSerializer;
import org.springframework.util.backoff.FixedBackOff;

/**
 * What happens when a @KafkaListener throws an exception?
 *
 * <ol>
 *   <li>The DefaultErrorHandler retries the same record: 2 more attempts, 1 second apart.</li>
 *   <li>If it still fails, the DeadLetterPublishingRecoverer copies the record to "&lt;topic&gt;.DLT"
 *       (adding headers with the exception details), and the consumer moves on to the next record.</li>
 * </ol>
 *
 * Spring Boot automatically plugs this error handler bean into every @KafkaListener.
 */
@Configuration
public class KafkaErrorHandlingConfig {

    @Bean
    DefaultErrorHandler errorHandler(ProducerFactory<String, Object> producerFactory) {
        var recoverer = new DeadLetterPublishingRecoverer(
                deadLetterTemplate(producerFactory),
                (record, exception) -> new TopicPartition(record.topic() + ".DLT", record.partition()));

        return new DefaultErrorHandler(recoverer, new FixedBackOff(1000L, 2));
    }

    /**
     * A record that couldn't be deserialized (e.g. invalid JSON) only exists as raw byte[], while a normal
     * failed record is a Java object. This template can publish both to the DLT.
     */
    private KafkaTemplate<String, Object> deadLetterTemplate(ProducerFactory<String, Object> producerFactory) {
        Map<Class<?>, Serializer<?>> serializers = new LinkedHashMap<>();
        serializers.put(byte[].class, new ByteArraySerializer());
        serializers.put(Object.class, new JacksonJsonSerializer<>());

        return new KafkaTemplate<>(new DefaultKafkaProducerFactory<>(
                producerFactory.getConfigurationProperties(),
                new StringSerializer(),
                new DelegatingByTypeSerializer(serializers, true)));
    }
}
