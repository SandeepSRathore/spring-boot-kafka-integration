package com.example.kafkalab.events;

/**
 * Published to the "inventory" topic after the inventory service has checked the stock.
 */
public record InventoryEvent(
        String orderId,
        String product,
        int quantity,
        double amount,
        Status status,
        String reason) {

    public enum Status { RESERVED, OUT_OF_STOCK }
}
