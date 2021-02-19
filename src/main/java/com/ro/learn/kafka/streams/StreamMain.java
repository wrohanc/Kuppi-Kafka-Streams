package com.ro.learn.kafka.streams;


import com.ro.learn.kafka.Configs;
import com.ro.learn.kafka.util.Order;
import com.ro.learn.kafka.util.OrderLine;
import com.ro.learn.kafka.util.serdes.JSONDeserializer;
import com.ro.learn.kafka.util.serdes.JSONSerializer;
import com.ro.learn.kafka.util.serdes.JSONSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.kstream.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class StreamMain {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Configs.STREAMS_APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.STATE_DIR_CONFIG, Configs.STATE_DIR);
        props.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Order> validOrderStream = builder.stream(Configs.VALID_OUTBOUND_TOPIC,
                Consumed.with(Serdes.Integer(), new JSONSerde<>(Order.class)));

        KStream<Integer, Order> deliveryOrderStream = validOrderStream.filter((k, v) -> v != null && v.getDeliveryType() == 2); // 2 is delivery
        deliveryOrderStream.foreach((k, order) -> {
            System.out.println("Delivery order id : " + order.getOrderId());
        });
       // KStream<Integer, OrderLine> deliveryOrderLineStream = deliveryOrderStream.flatMapValues((order) -> {
        KGroupedStream<Integer, OrderLine> groupedOrderLines = deliveryOrderStream.flatMapValues((order) -> {
                    if(order.getOrderLines()!= null){
                        return order.getOrderLines();
                    } else {
                        return new ArrayList<>(1);
                    }
                })
                .map((k,v) -> KeyValue.pair(v.getProductId(), v))
                .groupByKey(Grouped.with(Serdes.Integer(), new JSONSerde<>(OrderLine.class)))
                ;

//        KGroupedStream<Integer, OrderLine> groupedOrderLines = deliveryOrderLineStream
//                .groupByKey(Grouped.with(Serdes.Integer(), OrderLine()));

        KTable<Integer, OrderLine> productCount = groupedOrderLines.reduce((aggValue, newValue) -> {
            aggValue.setQuantity(aggValue.getQuantity() + newValue.getQuantity());
            return aggValue;
        });

//        groupedOrderLines.count().toStream().foreach((k, sum) -> {
//            System.out.println("Product " + k + " tot qty : " + sum);
//        });

        productCount.toStream().foreach((k, sum) -> {
            System.out.println("Product " + k + " tot qty : " + sum.getQuantity());
        });

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }

    static final class OrderLineSerde extends Serdes.WrapperSerde<OrderLine> {
        OrderLineSerde() {
            super(new JSONSerializer(), new JSONDeserializer(OrderLine.class));
        }
    }

    public static Serde<OrderLine> OrderLine() {
        OrderLineSerde serde = new OrderLineSerde();

        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put(JSONDeserializer.VALUE_CLASS_NAME_CONFIG, OrderLine.class);
        serde.configure(serdeConfigs, false);

        return serde;
    }
}
