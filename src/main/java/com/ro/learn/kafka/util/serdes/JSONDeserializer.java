package com.ro.learn.kafka.util.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JSONDeserializer<T> implements Deserializer<T> {
    private ObjectMapper objectMapper = new ObjectMapper();
    private Class<T> clazz;
    public static final String KEY_CLASS_NAME_CONFIG = "key.class.name";
    public static final String VALUE_CLASS_NAME_CONFIG = "value.class.name";

    public JSONDeserializer(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public void configure(Map configs, boolean isKey) {
    }

    @Override
    public T deserialize(String topic, byte[] message) {
        if (message == null) {
            return null;
        }
        try {
            Object o = objectMapper.readValue(message, clazz);
            return (T) o;
        } catch (Exception e) {
            throw new SerializationException(e);
        }
    }

}
