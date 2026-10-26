package com.example.kafkalab.order;

import java.util.Collection;
import java.util.List;
import java.util.stream.IntStream;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/orders")
public class OrderController {

    private static final List<String> PRODUCTS = List.of("LAPTOP", "PHONE", "HEADPHONES");

    private final OrderService orderService;

    public OrderController(OrderService orderService) {
        this.orderService = orderService;
    }

    @PostMapping
    public Order placeOrder(@RequestBody OrderRequest request) {
        return orderService.placeOrder(request);
    }

    /** Fire many small orders at once: handy for watching partitions and parallel consumers. */
    @PostMapping("/bulk")
    public List<Order> placeBulk(@RequestParam(defaultValue = "10") int count) {
        return IntStream.range(0, count)
                .mapToObj(i -> new OrderRequest(PRODUCTS.get(i % PRODUCTS.size()), 1, 100))
                .map(orderService::placeOrder)
                .toList();
    }

    @GetMapping
    public Collection<Order> allOrders() {
        return orderService.findAll();
    }

    @GetMapping("/{id}")
    public ResponseEntity<Order> order(@PathVariable String id) {
        return ResponseEntity.of(orderService.findById(id));
    }
}
