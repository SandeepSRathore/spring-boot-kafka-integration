package com.example.kafkalab.order;

import java.time.Instant;
import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
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
 * PRODUCER: publishes an OrderEvent for every new order.
 * CONSUMER: listens to inventory + payment results to keep the order status up to date.
 */
@Service
public class OrderService {

    private static final Logger log = LoggerFactory.getLogger(OrderService.class);

    // In-memory "database" to keep the example focused on Kafka
    private final Map<String, Order> orders = new ConcurrentHashMap<>();

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public OrderService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Order placeOrder(OrderRequest request) {
        String orderId = UUID.randomUUID().toString().substring(0, 8);
        Order order = new Order(orderId, request.product(), request.quantity(), request.amount(), OrderStatus.PLACED);
        orders.put(orderId, order);

        var event = new OrderEvent(orderId, request.product(), request.quantity(), request.amount(), Instant.now());

        // The KEY decides the partition: same key -> same partition -> events for one order stay in order.
        // send() is asynchronous: it returns immediately, the callback runs once the broker has acknowledged.
        kafkaTemplate.send(Topics.ORDERS, orderId, event)
                .whenComplete((result, ex) -> {
                    if (ex != null) {
                        log.error("[ORDER] failed to publish order {}", orderId, ex);
                    } else {
                        var meta = result.getRecordMetadata();
                        log.info("[ORDER] published {} -> {}-{} @offset {}",
                                orderId, meta.topic(), meta.partition(), meta.offset());
                    }
                });
        return order;
    }

    @KafkaListener(id = "order-inventory", topics = Topics.INVENTORY, groupId = "order-service")
    void onInventoryEvent(ConsumerRecord<String, InventoryEvent> record) {
        InventoryEvent event = record.value();
        if (event.status() == InventoryEvent.Status.OUT_OF_STOCK) {
            updateStatus(event.orderId(), OrderStatus.OUT_OF_STOCK);
        }
        // RESERVED: nothing to do yet, we wait for the payment result
    }

    @KafkaListener(id = "order-payments", topics = Topics.PAYMENTS, groupId = "order-service")
    void onPaymentEvent(ConsumerRecord<String, PaymentEvent> record) {
        PaymentEvent event = record.value();
        updateStatus(event.orderId(), event.status() == PaymentEvent.Status.COMPLETED
                ? OrderStatus.CONFIRMED
                : OrderStatus.PAYMENT_FAILED);
    }

    private void updateStatus(String orderId, OrderStatus status) {
        Order updated = orders.computeIfPresent(orderId, (id, order) -> order.withStatus(status));
        if (updated == null) {
            // Orders live in memory, so after a restart (or an offset reset) we can see events for unknown orders
            log.warn("[ORDER] ignoring {} for unknown order {}", status, orderId);
        } else {
            log.info("[ORDER] order {} is now {}", orderId, status);
        }
    }

    public Collection<Order> findAll() {
        return orders.values();
    }

    public Optional<Order> findById(String id) {
        return Optional.ofNullable(orders.get(id));
    }
}
