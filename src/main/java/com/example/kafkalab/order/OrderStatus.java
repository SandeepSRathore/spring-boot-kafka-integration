package com.example.kafkalab.order;

public enum OrderStatus {
    PLACED,          // order event published, waiting for inventory + payment
    CONFIRMED,       // stock reserved and payment completed
    OUT_OF_STOCK,    // inventory rejected it
    PAYMENT_FAILED   // payment rejected it (inventory releases the stock again)
}
