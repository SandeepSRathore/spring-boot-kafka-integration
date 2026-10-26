package com.example.kafkalab.inventory;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.example.kafkalab.events.InventoryEvent;
import com.example.kafkalab.events.OrderEvent;
import com.example.kafkalab.events.PaymentEvent;
import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * CONSUMER of "orders": tries to reserve stock for each order.
 * PRODUCER of "inventory": tells everyone whether the stock was reserved.
 * CONSUMER of "payments": gives the stock back if the payment failed (a "compensating action").
 */
@Service
public class InventoryService {

    private static final Logger log = LoggerFactory.getLogger(InventoryService.class);

    private final Map<String, Integer> stock = new ConcurrentHashMap<>(Map.of(
            "LAPTOP", 10,
            "PHONE", 20,
            "HEADPHONES", 5));

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public InventoryService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * concurrency = 3 starts 3 consumer threads in the "inventory-service" group.
     * Kafka gives each thread one of the 3 partitions of "orders", so 3 orders can be processed in parallel.
     */
    @KafkaListener(id = "inventory-orders", topics = Topics.ORDERS, groupId = "inventory-service", concurrency = "3")
    void onOrderEvent(ConsumerRecord<String, OrderEvent> record) {
        OrderEvent order = record.value();
        log.info("[INVENTORY] got order {} ({} x {}) from {}-{} @offset {}",
                order.orderId(), order.quantity(), order.product(),
                record.topic(), record.partition(), record.offset());

        // Experiment hook: order the product "CRASH" to see retries + the Dead Letter Topic in action
        if ("CRASH".equals(order.product())) {
            throw new IllegalStateException("Simulated failure while processing order " + order.orderId());
        }

        boolean reserved = tryReserve(order.product(), order.quantity());
        var event = new InventoryEvent(order.orderId(), order.product(), order.quantity(), order.amount(),
                reserved ? InventoryEvent.Status.RESERVED : InventoryEvent.Status.OUT_OF_STOCK,
                reserved ? null : "Not enough " + order.product() + " in stock");

        kafkaTemplate.send(Topics.INVENTORY, order.orderId(), event);
        log.info("[INVENTORY] order {} -> {} (stock now {})", order.orderId(), event.status(), stock);
    }

    @KafkaListener(id = "inventory-payments", topics = Topics.PAYMENTS, groupId = "inventory-service")
    void onPaymentEvent(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent payment = record.value();
        if (payment.status() == PaymentEvent.Status.FAILED) {
            stock.merge(payment.product(), payment.quantity(), Integer::sum);
            log.info("[INVENTORY] payment failed for order {}, released {} x {} (stock now {})",
                    payment.orderId(), payment.quantity(), payment.product(), stock);
        }
    }

    /** Several listener threads call this at the same time, so the check-and-update must be atomic. */
    private boolean tryReserve(String product, int quantity) {
        var reserved = new boolean[1];
        stock.computeIfPresent(product, (p, available) -> {
            if (available >= quantity) {
                reserved[0] = true;
                return available - quantity;
            }
            return available;
        });
        return reserved[0];
    }

    public Map<String, Integer> currentStock() {
        return Map.copyOf(stock);
    }
}
