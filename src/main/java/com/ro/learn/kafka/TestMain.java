package com.ro.learn.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.ro.learn.kafka.util.Order;
import org.apache.kafka.common.errors.SerializationException;

public class TestMain {
    public static void main(String[] args) {
        String json = "{\"orderId\": 1, \"customerId\": 1, \"orderLines\": [{\"lineId\": 1, \"productId\": 1, \"quantity\": 1, \"price\": 10}, {\"lineId\": 2, \"productId\": 2, \"quantity\": 2, \"price\": 20}]}";
        ObjectMapper mapper = new ObjectMapper();
        try {
            Order order =  mapper.readValue(json, Order.class);
            System.out.println(order);
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }
}
