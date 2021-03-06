package com.ro.learn.kafka.util.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class JSONSerde<T> implements Serializer<T>, Deserializer<T>, Serde<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class clazz;

    public JSONSerde(Class clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public T deserialize(String s, byte[] message) {
        if (message == null || message.length < 10) {
            return null;
        }
        System.out.println("Before deserialize : " +  new String(message));
        try {
            Object o = objectMapper.readValue(new String(message), clazz);
            return (T) o;
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(String topic, T message) {
        if (message == null) {
            return null;
        }

        try {
            byte[] str = objectMapper.writeValueAsString(message).getBytes();
            System.out.println("After serialize : " + new String(str));
            return str;
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {

    }

    @Override
    public Serializer<T> serializer() {
        return this;
    }

    @Override
    public Deserializer<T> deserializer() {
        return this;
    }
}
