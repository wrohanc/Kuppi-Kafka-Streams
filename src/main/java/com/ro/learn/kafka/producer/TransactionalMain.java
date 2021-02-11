package com.ro.learn.kafka.producer;

import com.ro.learn.kafka.Configs;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Properties;

public class TransactionalMain {
    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "MSG_PRODUCER");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "TXN");

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(props);
        producer.initTransactions();
        try {
            logger.info("Start sending messages...");
            producer.beginTransaction();
            for (int i = 1; i <= 100; i++) {
                producer.send(new ProducerRecord<>(Configs.INBOUND_TOPIC, i, "Web Order - " + i));
            }
            producer.commitTransaction();
        } catch (Exception e){
            logger.error("Exception in Transaction. Aborting...");
            producer.abortTransaction();
            producer.close();
            throw new RuntimeException(e);
        }

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Producer");
            producer.close();
        }));
    }
}
