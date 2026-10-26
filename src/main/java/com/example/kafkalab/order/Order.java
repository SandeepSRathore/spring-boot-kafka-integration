package com.example.kafkalab.order;

public record Order(
        String id,
        String product,
        int quantity,
        double amount,
        OrderStatus status) {

    public Order withStatus(OrderStatus newStatus) {
        return new Order(id, product, quantity, amount, newStatus);
    }
}
