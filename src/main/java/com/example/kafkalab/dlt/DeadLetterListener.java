package com.example.kafkalab.dlt;

import java.nio.charset.StandardCharsets;

import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.stereotype.Component;

/**
 * Reads the Dead Letter Topics so you can see what failed and why.
 *
 * <p>The value is read as a plain String (overriding the JSON deserializer) because a DLT can contain
 * messages that are not valid JSON at all.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    @KafkaListener(id = "dlt-inspector", topics = {Topics.ORDERS_DLT, Topics.INVENTORY_DLT, Topics.PAYMENTS_DLT},
            groupId = "dlt-inspector",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    void onDeadLetter(ConsumerRecord<String, String> record) {
        // The DeadLetterPublishingRecoverer added these headers when it copied the record here
        log.warn("[DLT] {} key={} value={} | failed in group '{}' with: {}",
                record.topic(), record.key(), record.value(),
                header(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP),
                header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE));
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "?" : new String(header.value(), StandardCharsets.UTF_8);
    }
}
