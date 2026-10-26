package com.example.kafkalab.order;

public record OrderRequest(String product, int quantity, double amount) {
}
