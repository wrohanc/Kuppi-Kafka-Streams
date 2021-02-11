package com.ro.learn.kafka;

public class Configs {
    public static final String VALIDATOR_APP_ID = "VALIDATOR";
    public static final String STREAMS_APP_ID = "STREAM";
    public static final String BOOTSTRAP_SERVERS = "localhost:9092,localhost:9093";
    public static final String INBOUND_TOPIC = "web-orders-new";
    public static final String VALID_OUTBOUND_TOPIC = "web-orders-validated";
    public static final String INVALID_OUTBOUND_TOPIC = "web-orders-invalidated";
    public static final String PICKUP_TOPIC = "pickup-orders";
    public static final String DELIVERY_TOPIC = "delivery-orders";
}
