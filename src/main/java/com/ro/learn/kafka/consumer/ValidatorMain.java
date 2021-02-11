package com.ro.learn.kafka.consumer;

import com.ro.learn.kafka.Configs;
import com.ro.learn.kafka.util.serdes.JSONDeserializer;
import com.ro.learn.kafka.util.serdes.JSONSerializer;
import com.ro.learn.kafka.util.Order;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ValidatorMain {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.CLIENT_ID_CONFIG, Configs.VALIDATOR_APP_ID);
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSONDeserializer.class);
        consumerProps.put(JSONDeserializer.VALUE_CLASS_NAME_CONFIG, Order.class);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "L1_VALIDATOR");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Arrays.asList(Configs.INBOUND_TOPIC));

        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.CLIENT_ID_CONFIG, Configs.VALIDATOR_APP_ID);
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerializer.class);

        KafkaProducer<Integer, Order> producer = new KafkaProducer<>(producerProps);

        while (true) {
            ConsumerRecords<String, Order> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, Order> record : records) {
                if(record.value().getCustomerId() > 0) {
                    record.value().setValid(true);
                    producer.send(new ProducerRecord<>(Configs.VALID_OUTBOUND_TOPIC, record.value().getOrderId(),
                            record.value()));
                } else {
                    producer.send(new ProducerRecord<>(Configs.INVALID_OUTBOUND_TOPIC, record.value().getOrderId(),
                            record.value()));
                }
            }
        }
    }
}
