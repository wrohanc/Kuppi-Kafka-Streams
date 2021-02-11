package com.ro.learn.kafka.streams;

import com.ro.learn.kafka.Configs;
import com.ro.learn.kafka.util.Order;
import com.ro.learn.kafka.util.serdes.JSONDeserializer;
import com.ro.learn.kafka.util.serdes.JSONSerde;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Properties;

public class StreamMain {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Configs.STREAMS_APP_ID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);

        StreamsBuilder builder = new StreamsBuilder();
        KStream<Integer, Order> KS0 = builder.stream(Configs.VALID_OUTBOUND_TOPIC,
                Consumed.with(Serdes.Integer(), new JSONSerde<>(Order.class)));
        //branch
        KStream<Integer, Order>[] branches = KS0.branch((k, v) -> v.getDeliveryType() == 1, (k, v) -> v.getDeliveryType() == 2);

        KStream<Integer, Order> KS1_1 = branches[0];
        KStream<Integer, Order> KS1_2 = branches[1];

        System.out.println("Pickup orders branch: ");
        KS1_1.print(Printed.toSysOut());

        System.out.println("Delivery orders branch: ");
        KS1_2.print(Printed.toSysOut());

        //filter
        KS0.filter((k, v) ->
                v.getDeliveryType() == 1) // 1 is pickup
                .to(Configs.PICKUP_TOPIC, Produced.with(Serdes.Integer(), new JSONSerde<>(Order.class)));

        KS0.filter((k, v) ->
                v.getDeliveryType() == 2) // 2 is delivery
                .to(Configs.DELIVERY_TOPIC, Produced.with(Serdes.Integer(), new JSONSerde<>(Order.class)));


        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }
}
