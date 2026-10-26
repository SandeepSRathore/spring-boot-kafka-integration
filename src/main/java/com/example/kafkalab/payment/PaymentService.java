package com.example.kafkalab.payment;

import com.example.kafkalab.events.InventoryEvent;
import com.example.kafkalab.events.PaymentEvent;
import com.example.kafkalab.events.Topics;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

/**
 * CONSUMER of "inventory": charges the customer once the stock is reserved.
 * PRODUCER of "payments": publishes whether the payment worked.
 */
@Service
public class PaymentService {

    private static final Logger log = LoggerFactory.getLogger(PaymentService.class);

    // Pretend every customer has this much money
    private static final double CUSTOMER_BALANCE = 1000.0;

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public PaymentService(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(id = "payment-inventory", topics = Topics.INVENTORY, groupId = "payment-service")
    void onInventoryEvent(ConsumerRecord<String, InventoryEvent> record) {
        InventoryEvent inventory = record.value();
        if (inventory.status() != InventoryEvent.Status.RESERVED) {
            return; // nothing to charge
        }

        boolean paid = inventory.amount() <= CUSTOMER_BALANCE;
        var event = new PaymentEvent(inventory.orderId(), inventory.product(), inventory.quantity(), inventory.amount(),
                paid ? PaymentEvent.Status.COMPLETED : PaymentEvent.Status.FAILED,
                paid ? null : "Insufficient funds: amount " + inventory.amount() + " > balance " + CUSTOMER_BALANCE);

        kafkaTemplate.send(Topics.PAYMENTS, inventory.orderId(), event);
        log.info("[PAYMENT] order {} amount {} -> {}", inventory.orderId(), inventory.amount(), event.status());
    }
}
