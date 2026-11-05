package com.example.kafkalab.dlt;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.mail.MailException;
import org.springframework.mail.SimpleMailMessage;
import org.springframework.mail.javamail.JavaMailSender;
import org.springframework.stereotype.Component;

/**
 * Reads the Dead Letter Topics so you can see what failed and why, and emails an alert for every record.
 *
 * <p>The value is read as a plain String (overriding the JSON deserializer) because a DLT can contain
 * messages that are not valid JSON at all.
 */
@Component
public class DeadLetterListener {

    private static final Logger log = LoggerFactory.getLogger(DeadLetterListener.class);

    private final JavaMailSender mailSender;
    private final String alertFrom;
    private final String alertTo;

    DeadLetterListener(JavaMailSender mailSender,
                       @Value("${kafkalab.dlt-alert.from}") String alertFrom,
                       @Value("${kafkalab.dlt-alert.to}") String alertTo) {
        this.mailSender = mailSender;
        this.alertFrom = alertFrom;
        this.alertTo = alertTo;
    }

    @KafkaListener(id = "dlt-inspector", topics = {Topics.ORDERS_DLT, Topics.INVENTORY_DLT, Topics.PAYMENTS_DLT},
            groupId = "dlt-inspector",
            properties = "value.deserializer=org.apache.kafka.common.serialization.StringDeserializer")
    void onDeadLetter(ConsumerRecord<String, String> record) {
        // The DeadLetterPublishingRecoverer added these headers when it copied the record here
        String group = header(record, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP);
        String error = header(record, KafkaHeaders.DLT_EXCEPTION_MESSAGE);

        log.warn("[DLT] {} key={} value={} | failed in group '{}' with: {}",
                record.topic(), record.key(), record.value(), group, error);

        sendAlert(record, group, error);
    }

    private void sendAlert(ConsumerRecord<String, String> record, String group, String error) {
        var mail = new SimpleMailMessage();
        mail.setFrom(alertFrom);
        mail.setTo(alertTo);
        mail.setSubject("[DLT] %s: message %s failed".formatted(record.topic(), record.key()));
        mail.setText("""
                A message could not be processed and was moved to a dead letter topic.

                DLT topic:      %s (partition %d, offset %d)
                Original topic: %s (partition %s, offset %s)
                Consumer group: %s
                Key:            %s
                Error:          %s

                Payload:
                %s
                """.formatted(
                record.topic(), record.partition(), record.offset(),
                header(record, KafkaHeaders.DLT_ORIGINAL_TOPIC),
                binaryHeader(record, KafkaHeaders.DLT_ORIGINAL_PARTITION),
                binaryHeader(record, KafkaHeaders.DLT_ORIGINAL_OFFSET),
                group, record.key(), error, record.value()));
        try {
            mailSender.send(mail);
            log.info("[DLT] alert email sent to {} for key={}", alertTo, record.key());
        } catch (MailException e) {
            // Don't rethrow: the record is already safe in the DLT, and failing here would make Kafka retry
            // this DLT record (and try to dead-letter the dead letter). Log loudly instead.
            log.error("[DLT] could not send alert email for {} key={}: {}",
                    record.topic(), record.key(), e.getMessage());
        }
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? "?" : new String(header.value(), StandardCharsets.UTF_8);
    }

    /** Partition (4-byte int) and offset (8-byte long) headers are stored as raw numbers, not text. */
    private static String binaryHeader(ConsumerRecord<?, ?> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return "?";
        }
        ByteBuffer bytes = ByteBuffer.wrap(header.value());
        return switch (header.value().length) {
            case Integer.BYTES -> String.valueOf(bytes.getInt());
            case Long.BYTES -> String.valueOf(bytes.getLong());
            default -> "?";
        };
    }
}
