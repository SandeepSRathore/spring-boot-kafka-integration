package com.example.kafkalab.events;

/**
 * All topic names in one place.
 */
public final class Topics {

    public static final String ORDERS = "orders";
    public static final String INVENTORY = "inventory";
    public static final String PAYMENTS = "payments";

    // Dead Letter Topics: where messages go after all retries have failed
    public static final String ORDERS_DLT = ORDERS + ".DLT";
    public static final String INVENTORY_DLT = INVENTORY + ".DLT";
    public static final String PAYMENTS_DLT = PAYMENTS + ".DLT";

    private Topics() {
    }
}
