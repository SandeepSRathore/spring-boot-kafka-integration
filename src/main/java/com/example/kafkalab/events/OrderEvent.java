package com.example.kafkalab.events;

import java.time.Instant;

/**
 * Published to the "orders" topic when a customer places an order.
 */
public record OrderEvent(
        String orderId,
        String product,
        int quantity,
        double amount,
        Instant createdAt) {
}
