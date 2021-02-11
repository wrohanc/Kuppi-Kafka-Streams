package com.ro.learn.kafka.util;

import com.ro.learn.kafka.util.serdes.JSONSerdeCompatible;

import java.time.LocalDateTime;
import java.util.List;

public class Order implements JSONSerdeCompatible {
    private int orderId;
    private LocalDateTime createdTime;
    private List<OrderLine> orderLines;
    private int customerId;
    private boolean valid;
    private int deliveryType;

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public LocalDateTime getCreatedTime() {
        return createdTime;
    }

    public void setCreatedTime(LocalDateTime createdTime) {
        this.createdTime = createdTime;
    }

    public List<OrderLine> getOrderLines() {
        return orderLines;
    }

    public void setOrderLines(List<OrderLine> orderLines) {
        this.orderLines = orderLines;
    }

    public int getCustomerId() {
        return customerId;
    }

    public void setCustomerId(int customerId) {
        this.customerId = customerId;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public int getDeliveryType() {
        return deliveryType;
    }

    public void setDeliveryType(int deliveryType) {
        this.deliveryType = deliveryType;
    }
}
