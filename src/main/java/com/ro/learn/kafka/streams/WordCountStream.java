package com.ro.learn.kafka.streams;

import com.ro.learn.kafka.Configs;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.Arrays;
import java.util.Locale;
import java.util.Properties;

public class WordCountStream {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "WORDCOUNT");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Configs.BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,Serdes.String().getClass());
        props.put(StreamsConfig.STATE_DIR_CONFIG,"tmp/state_dir");

        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> inputStream = streamsBuilder.stream("WORDCOUNT_IN");
        KStream<String, String> wordStream = inputStream
                .flatMapValues((val) -> Arrays.asList(val.split(" ")))
                .mapValues((k, v) -> v.toLowerCase(Locale.ROOT));
        KGroupedStream<String, String> groupedStream = wordStream.groupBy((k,v) -> v);
        //KGroupedStream<String, String> groupedStream = wordStream.groupBy((k,v) -> String.valueOf(v.charAt(0)));
        KTable<String, Long> countTable = groupedStream.count();
        countTable.toStream().print(Printed.<String,Long>toSysOut().withLabel("Word count"));

        KafkaStreams streams = new KafkaStreams(streamsBuilder.build(), props);
        streams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            streams.close();
        }));
    }
}
