# Kafka Lab: Spring Boot + Kafka, learn by experimenting

A small Spring Boot app for learning Kafka. Four "services" (order, inventory, payment, notification)
live in **one app, in separate packages**, and talk to each other **only through Kafka topics**,
just like real microservices would. The business logic is kept tiny on purpose, so the code shows the Kafka concepts and not much else.

## The flow

```
  POST /orders
       │
       ▼
 ┌──────────────┐  OrderEvent   ┌──────────────────┐  InventoryEvent  ┌────────────────┐  PaymentEvent
 │ OrderService │──"orders"────▶│ InventoryService │──"inventory"────▶│ PaymentService │──"payments"──┐
 └──────────────┘               └──────────────────┘                  └────────────────┘              │
        ▲  ▲                         ▲      (reserve stock)               (charge, max 1000)            │
        │  │                         └─────── payment FAILED → release stock ◀──────────────────────────┤
        │  └── OUT_OF_STOCK ── "inventory"                                                            │
        └───── COMPLETED / FAILED ── "payments" ◀─────────────────────────────────────────────────────┘

 NotificationService listens to ALL three topics and "emails" the customer.
 DeadLetterListener listens to orders.DLT / inventory.DLT / payments.DLT.
```

| Order status     | When                                            |
|------------------|-------------------------------------------------|
| `PLACED`         | event published, waiting for the other services |
| `CONFIRMED`      | stock reserved and payment completed            |
| `OUT_OF_STOCK`   | not enough stock (start: LAPTOP 10, PHONE 20, HEADPHONES 5) |
| `PAYMENT_FAILED` | amount > 1000; the stock is put back            |

## Where each concept lives

| Concept | File |
|---|---|
| Kafka in Docker (KRaft, no ZooKeeper) + web UI | `docker-compose.yml` |
| Serializers, deserializers, `auto-offset-reset` | `src/main/resources/application.yml` |
| Creating topics with partitions | `config/KafkaTopicConfig.java` |
| **Producer**, message keys, async send callback | `order/OrderService.java` → `placeOrder` |
| **Consumer** with `@KafkaListener`, `ConsumerRecord` (key / partition / offset) | every `*Service.java` |
| Consume → process → produce (event chain) | `inventory/InventoryService.java`, `payment/PaymentService.java` |
| **Consumer groups** (several groups read the same topic independently) | `groupId = ...` on each listener |
| **Concurrency** (one thread per partition) | `concurrency = "3"` in `InventoryService` |
| One listener on many topics + one topic with many event types | `notification/NotificationService.java` |
| **Retries + Dead Letter Topic** | `config/KafkaErrorHandlingConfig.java`, `dlt/DeadLetterListener.java` |
| **Poison pill** handling (bad JSON) | `ErrorHandlingDeserializer` in `application.yml` |
| Compensating action (undo a step when a later one fails) | `InventoryService.onPaymentEvent` |

## Run it

Prerequisites: Java 25, Maven, Docker.

```bash
docker compose up -d          # Kafka on localhost:9092, Kafka UI on http://localhost:8090
mvn spring-boot:run           # the app on http://localhost:8080
```

Place some orders:

```bash
# happy path -> CONFIRMED
curl -s -XPOST localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"product":"LAPTOP","quantity":2,"amount":500}'

# not enough stock -> OUT_OF_STOCK
curl -s -XPOST localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"product":"HEADPHONES","quantity":50,"amount":100}'

# too expensive -> PAYMENT_FAILED (and the stock is released again)
curl -s -XPOST localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"product":"PHONE","quantity":1,"amount":5000}'

curl -s localhost:8080/orders       # all orders and their status
curl -s localhost:8080/inventory    # current stock
curl -s -XPOST "localhost:8080/orders/bulk?count=12"   # 12 quick orders
```

Then **read the app logs**: every line says which thread, topic, partition and offset handled what.
Also open **Kafka UI** at http://localhost:8090 to browse topics, messages (including headers like
`__TypeId__`), partitions and consumer groups.

The Kafka CLI tools are inside the container, under `/opt/kafka/bin`:

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:9092 --list
```

---

## Experiments

Each experiment is small. Change one thing, then watch the logs and Kafka UI.

### 1. Keys decide partitions
Run `POST /orders/bulk?count=12` and look at the `[ORDER] published ... -> orders-X` lines.
- Orders spread across `orders-0/1/2` because each order has a different key (its `orderId`).
- Follow one orderId: its `orders`, `inventory` and `payments` events all land on the **same partition
  number**, because the topics have the same partition count and Kafka hashes the same key the same way.
- 💡 Kafka only guarantees ordering **within a partition**, which is why you choose the key carefully
  (e.g. `orderId`: all events of one order stay in order).

### 2. Consumer groups = publish/subscribe
`InventoryService` (group `inventory-service`) and `NotificationService` (group `notification-service`)
both read `orders`, and **both get every message**. Each group keeps its own offsets.

```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
       --describe --group inventory-service
```
Look at `CURRENT-OFFSET`, `LOG-END-OFFSET` and `LAG` (= messages not processed yet).

### 3. Concurrency: threads vs partitions
In `InventoryService`, the `orders` listener uses `concurrency = "3"`. At startup look for
`partitions assigned`: each thread `inventory-orders-0/1/2-C-1` gets one partition.
- Change it to `"1"` → one thread reads all 3 partitions, one record at a time.
- Change it to `"5"` → 2 threads get **no partition** and sit idle.
  💡 Within a group, partitions are the upper limit on parallelism.

### 4. Scale out: run a second instance (rebalancing)
```bash
mvn spring-boot:run -Dspring-boot.run.arguments=--server.port=8081
```
Both instances join the same groups. Watch both logs: partitions are **revoked** and **reassigned**
(a *rebalance*). Now there are 6 consumers for 3 partitions, so some stay idle. Stop the second
instance (Ctrl+C) and its partitions move back to the first one.

### 5. Kafka keeps messages while consumers are down
1. Stop the app.
2. Publish an order straight to Kafka. The `__TypeId__` header tells the JSON deserializer which class to build:
   ```bash
   printf '__TypeId__:com.example.kafkalab.events.OrderEvent\tcli-1:{"orderId":"cli-1","product":"PHONE","quantity":1,"amount":99}\n' \
     | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server localhost:9092 \
         --topic orders --property parse.key=true --property key.separator=: --property parse.headers=true
   ```
3. `kafka-consumer-groups.sh --describe --group inventory-service` now shows `LAG 1`.
4. Start the app again. It picks up exactly where it left off and processes `cli-1`.
   (`OrderService` logs *"ignoring CONFIRMED for unknown order"*, because orders are only kept in memory.)

### 6. Replay: rewind a consumer group
Kafka doesn't delete messages after they're read, so you can rewind. Stop the app first (a group must be inactive to reset it):
```bash
docker exec -it kafka /opt/kafka/bin/kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
       --group notification-service --reset-offsets --to-earliest --all-topics --execute
```
Start the app: every notification is "sent" again, while the other groups don't re-process anything.
Also try `--to-offset 2 --topic orders:1` or `--shift-by -3`.

### 7. Retries and the Dead Letter Topic
```bash
curl -s -XPOST localhost:8080/orders -H 'Content-Type: application/json' \
     -d '{"product":"CRASH","quantity":1,"amount":10}'
```
`InventoryService` throws for the product `CRASH`. You'll see:
1. `got order ... CRASH` **3 times**, 1 second apart (1 attempt + 2 retries, `FixedBackOff(1000, 2)`).
2. Then a `[DLT] orders.DLT ...` line: the record was moved to the dead letter topic, with the
   exception in its headers (look at them in Kafka UI).
3. The consumer carries on with the next records. The order stays `PLACED` forever. In real life
   someone would inspect the DLT and fix or replay it.

Try: change the backoff in `KafkaErrorHandlingConfig`, or call
`errorHandler.addNotRetryableExceptions(IllegalStateException.class)` to skip retries for errors that
will never succeed anyway.

💡 While a record is being retried, its **whole partition is blocked**. Orders on other partitions keep flowing (watch the other threads).

### 8. Poison pill (a message that can't be deserialized)
```bash
echo "this is not json" | docker exec -i kafka /opt/kafka/bin/kafka-console-producer.sh \
       --bootstrap-server localhost:9092 --topic orders
```
Without `ErrorHandlingDeserializer` the consumer would fail on this message forever. With it, the
message goes **straight to `orders.DLT`** (no retries: bad bytes won't get better). It shows up
**twice**, once from each group that reads `orders` (`inventory-service` and `notification-service`).

### 9. Look inside a topic
```bash
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 \
       --topic payments --from-beginning \
       --property print.key=true --property print.partition=true --property print.offset=true --property print.headers=true
```
`kafka-console-consumer` uses its own temporary group, so the app doesn't notice it.

### 10. More ideas to try on your own
- `kafka-topics.sh --alter --topic orders --partitions 6`: what happens to key → partition mapping? (Hint: existing keys can move.)
- Set `spring.kafka.consumer.auto-offset-reset: latest`, rename a `groupId`, restart: old messages are skipped.
- Make `PaymentService` slow (`Thread.sleep(5000)`) and watch the `payment-service` lag grow.
- Add a new `ShippingService` that listens to `payments` with its own group. No other code needs to change.

---

## Tests

```bash
mvn test        # no Docker needed: @EmbeddedKafka starts an in-memory broker
```

`OrderFlowIntegrationTest` starts the whole app and checks each path through the order flow:
confirmed, out of stock, payment failed (with the stock released), all events of an order on the same
partition, and retries ending in `orders.DLT`. Ideas worth borrowing for your own Kafka tests:
- **Awaitility** (`await().untilAsserted(...)`): Kafka is asynchronous, so wait for the result instead of `Thread.sleep`.
- A **plain test consumer** in its own group (`stringConsumer(...)`) reads topics as raw JSON to check keys, partitions and headers.

---

## Reset everything
```bash
docker compose down -v   # removes the Kafka container and all its data
```

## Project layout
```
src/main/java/com/example/kafkalab
├── KafkaLabApplication.java
├── config/        topics + error handling (retries, DLT)
├── events/        OrderEvent, InventoryEvent, PaymentEvent, Topics (the "contract" between services)
├── order/         REST API + producer of OrderEvent + consumer of results
├── inventory/     consumes orders, produces inventory events
├── payment/       consumes inventory events, produces payment events
├── notification/  consumes everything, logs customer messages
└── dlt/           reads the dead letter topics
```
