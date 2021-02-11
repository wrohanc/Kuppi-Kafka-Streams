package com.ro.learn.kafka.util;

import com.ro.learn.kafka.util.serdes.JSONSerdeCompatible;

public class OrderLine implements JSONSerdeCompatible {
    private int lineId;
    private int productId;
    private double quantity;
    private double price;

    public int getLineId() {
        return lineId;
    }

    public void setLineId(int lineId) {
        this.lineId = lineId;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public double getQuantity() {
        return quantity;
    }

    public void setQuantity(double quantity) {
        this.quantity = quantity;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }
}
