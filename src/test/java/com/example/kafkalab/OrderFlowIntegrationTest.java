package com.example.kafkalab;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;

import com.example.kafkalab.events.Topics;
import com.example.kafkalab.inventory.InventoryService;
import com.example.kafkalab.order.Order;
import com.example.kafkalab.order.OrderRequest;
import com.example.kafkalab.order.OrderService;
import com.example.kafkalab.order.OrderStatus;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

/**
 * Runs the whole app against an in-memory Kafka broker started by @EmbeddedKafka (no Docker needed).
 *
 * <p>Everything is asynchronous: placing an order only publishes an event. So each test places an order
 * and then WAITS (with Awaitility) until the other services have reacted.
 *
 * <p>All tests share one app, so they share the in-memory stock. That's why stock assertions compare
 * against the value read at the start of the test instead of hard-coded numbers.
 */
@SpringBootTest
@EmbeddedKafka(partitions = 3, bootstrapServersProperty = "spring.kafka.bootstrap-servers")
class OrderFlowIntegrationTest {

    @Autowired
    private OrderService orderService;

    @Autowired
    private InventoryService inventoryService;

    @Autowired
    private EmbeddedKafkaBroker broker;

    @Test
    void orderIsConfirmedWhenStockAndMoneyAreAvailable() {
        int laptopsBefore = stockOf("LAPTOP");

        Order order = orderService.placeOrder(new OrderRequest("LAPTOP", 2, 500));

        assertThat(order.status()).isEqualTo(OrderStatus.PLACED); // nothing has been processed yet
        awaitStatus(order, OrderStatus.CONFIRMED);
        assertThat(stockOf("LAPTOP")).isEqualTo(laptopsBefore - 2);
    }

    @Test
    void orderIsRejectedWhenThereIsNotEnoughStock() {
        int headphonesBefore = stockOf("HEADPHONES");

        Order order = orderService.placeOrder(new OrderRequest("HEADPHONES", 50, 100));

        awaitStatus(order, OrderStatus.OUT_OF_STOCK);
        assertThat(stockOf("HEADPHONES")).isEqualTo(headphonesBefore);
    }

    @Test
    void stockIsReleasedWhenPaymentFails() {
        int phonesBefore = stockOf("PHONE");

        Order order = orderService.placeOrder(new OrderRequest("PHONE", 1, 5000)); // more than the 1000 balance

        awaitStatus(order, OrderStatus.PAYMENT_FAILED);
        // The order service and the inventory service read "payments" in different consumer groups,
        // so the stock may be released a moment after the status changed: wait for it too.
        waitUpTo30s().untilAsserted(() -> assertThat(stockOf("PHONE")).isEqualTo(phonesBefore));
    }

    @Test
    void allEventsOfAnOrderAreKeyedByOrderIdAndLandOnTheSamePartition() {
        Order order = orderService.placeOrder(new OrderRequest("PHONE", 1, 100));
        awaitStatus(order, OrderStatus.CONFIRMED);

        try (Consumer<String, String> consumer = stringConsumer("test-partitions")) {
            consumer.subscribe(List.of(Topics.ORDERS, Topics.INVENTORY, Topics.PAYMENTS));

            List<ConsumerRecord<String, String>> events =
                    pollUntil(consumer, record -> order.id().equals(record.key()), 3);

            assertThat(events).extracting(ConsumerRecord::topic)
                    .containsExactlyInAnyOrder(Topics.ORDERS, Topics.INVENTORY, Topics.PAYMENTS);
            // Same key + same partition count => same partition number in every topic
            assertThat(events).extracting(ConsumerRecord::partition).containsOnly(events.getFirst().partition());
        }
    }

    @Test
    void failingOrderIsRetriedAndThenSentToTheDeadLetterTopic() {
        Order order = orderService.placeOrder(new OrderRequest("CRASH", 1, 10));

        try (Consumer<String, String> consumer = stringConsumer("test-dlt")) {
            consumer.subscribe(List.of(Topics.ORDERS_DLT));

            // Arrives after ~2 seconds: 1 attempt + 2 retries, 1 second apart
            ConsumerRecord<String, String> dead =
                    pollUntil(consumer, record -> order.id().equals(record.key()), 1).getFirst();

            assertThat(dead.value()).contains("\"product\":\"CRASH\"");
            assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_TOPIC)).isEqualTo(Topics.ORDERS);
            assertThat(header(dead, KafkaHeaders.DLT_ORIGINAL_CONSUMER_GROUP)).isEqualTo("inventory-service");
            assertThat(header(dead, KafkaHeaders.DLT_EXCEPTION_MESSAGE)).contains("Simulated failure");
        }

        // Inventory never answered, so the order is stuck
        assertThat(orderService.findById(order.id())).map(Order::status).contains(OrderStatus.PLACED);
    }

    // ---------------------------------------------------------------- helpers

    private int stockOf(String product) {
        return inventoryService.currentStock().get(product);
    }

    private void awaitStatus(Order order, OrderStatus expected) {
        waitUpTo30s().untilAsserted(() ->
                assertThat(orderService.findById(order.id())).map(Order::status).contains(expected));
    }

    /** Generous timeout: on the first test the consumers are still joining their groups. */
    private static ConditionFactory waitUpTo30s() {
        return await().atMost(Duration.ofSeconds(30));
    }

    /**
     * A plain Kafka consumer reading raw JSON strings, starting from the beginning of each topic
     * (KafkaTestUtils sets auto.offset.reset=earliest). It uses its own group, so the app doesn't notice it.
     */
    private Consumer<String, String> stringConsumer(String groupId) {
        var props = KafkaTestUtils.consumerProps(broker, groupId, false);
        return new DefaultKafkaConsumerFactory<>(props, new StringDeserializer(), new StringDeserializer())
                .createConsumer();
    }

    /** Keeps polling until {@code count} records matching the filter have been seen. */
    private static List<ConsumerRecord<String, String>> pollUntil(
            Consumer<String, String> consumer, Predicate<ConsumerRecord<String, String>> filter, int count) {
        List<ConsumerRecord<String, String>> matches = new ArrayList<>();
        // A KafkaConsumer must not be used by several threads, so keep Awaitility on the test thread
        waitUpTo30s().pollInSameThread().until(() -> {
            consumer.poll(Duration.ofMillis(500)).forEach(record -> {
                if (filter.test(record)) {
                    matches.add(record);
                }
            });
            return matches.size() >= count;
        });
        return matches;
    }

    private static String header(ConsumerRecord<?, ?> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
