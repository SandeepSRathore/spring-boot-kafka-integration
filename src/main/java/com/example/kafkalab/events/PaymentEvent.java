package com.example.kafkalab.events;

/**
 * Published to the "payments" topic after the payment service has tried to charge the customer.
 */
public record PaymentEvent(
        String orderId,
        String product,
        int quantity,
        double amount,
        Status status,
        String reason) {

    public enum Status { COMPLETED, FAILED }
}
