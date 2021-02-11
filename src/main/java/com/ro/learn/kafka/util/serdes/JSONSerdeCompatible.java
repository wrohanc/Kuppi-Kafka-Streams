package com.ro.learn.kafka.util.serdes;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.ro.learn.kafka.util.Order;
import com.ro.learn.kafka.util.OrderLine;

@JsonSubTypes({
        @JsonSubTypes.Type(value = Order.class, name = "order"),
        @JsonSubTypes.Type(value = OrderLine.class, name = "orderLine"),
})
public interface JSONSerdeCompatible {

}
