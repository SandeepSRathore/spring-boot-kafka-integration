package com.example.kafkalab.notification;

import com.example.kafkalab.events.InventoryEvent;
import com.example.kafkalab.events.OrderEvent;
import com.example.kafkalab.events.PaymentEvent;
import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

/**
 * CONSUMER only: one listener subscribed to all three topics, "sending" a message to the customer.
 *
 * <p>It is in its own consumer group, so it receives EVERY event, independently of the other services
 * that read the same topics (publish/subscribe).
 */
@Service
public class NotificationService {

    private static final Logger log = LoggerFactory.getLogger(NotificationService.class);

    @KafkaListener(id = "notification", topics = {Topics.ORDERS, Topics.INVENTORY, Topics.PAYMENTS},
            groupId = "notification-service")
    void onEvent(ConsumerRecord<String, Object> record) {
        // The JSON deserializer built the right class from the "__TypeId__" header
        String message = switch (record.value()) {
            case OrderEvent e -> "Thanks! We received your order " + e.orderId() + " for " + e.quantity() + " x " + e.product();
            case InventoryEvent e when e.status() == InventoryEvent.Status.OUT_OF_STOCK ->
                    "Sorry, order " + e.orderId() + " was cancelled: " + e.reason();
            case InventoryEvent e -> null; // stock reserved: internal detail, don't bother the customer
            case PaymentEvent e when e.status() == PaymentEvent.Status.COMPLETED ->
                    "Payment received, order " + e.orderId() + " is confirmed!";
            case PaymentEvent e -> "Payment for order " + e.orderId() + " failed: " + e.reason();
            default -> "Unknown event: " + record.value();
        };

        if (message != null) {
            log.info("[NOTIFICATION] (from {}) {}", record.topic(), message);
        }
    }
}
