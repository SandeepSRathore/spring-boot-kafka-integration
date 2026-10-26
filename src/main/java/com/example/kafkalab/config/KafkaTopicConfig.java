package com.example.kafkalab.config;

import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

/**
 * Spring Boot's KafkaAdmin creates every NewTopic bean on startup (if it doesn't exist yet).
 *
 * <p>3 partitions = up to 3 consumers in the same group can read a topic in parallel.
 * 1 replica, because our local cluster has only one broker.
 */
@Configuration
public class KafkaTopicConfig {

    private static final int PARTITIONS = 3;

    @Bean
    NewTopic ordersTopic() {
        return TopicBuilder.name(Topics.ORDERS).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic inventoryTopic() {
        return TopicBuilder.name(Topics.INVENTORY).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic paymentsTopic() {
        return TopicBuilder.name(Topics.PAYMENTS).partitions(PARTITIONS).replicas(1).build();
    }

    // A failed record goes to the same partition number in the DLT, so the DLT needs the same partition count.
    @Bean
    NewTopic ordersDltTopic() {
        return TopicBuilder.name(Topics.ORDERS_DLT).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic inventoryDltTopic() {
        return TopicBuilder.name(Topics.INVENTORY_DLT).partitions(PARTITIONS).replicas(1).build();
    }

    @Bean
    NewTopic paymentsDltTopic() {
        return TopicBuilder.name(Topics.PAYMENTS_DLT).partitions(PARTITIONS).replicas(1).build();
    }
}
