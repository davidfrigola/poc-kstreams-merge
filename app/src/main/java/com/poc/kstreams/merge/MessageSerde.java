package com.poc.kstreams.merge;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.util.Map;

public class MessageSerde implements Serializer<Message>, Deserializer<Message>, Serde<Message> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    @Override
    public void configure(final Map<String, ?> configs, final boolean isKey) {
    }

    @SuppressWarnings("unchecked")
    @Override
    public Message deserialize(final String topic, final byte[] data) {
        if (data == null) {
            return null;
        }

        try {
            return (Message) OBJECT_MAPPER.readValue(data, Message.class);
        } catch (final IOException e) {
            throw new SerializationException(e);
        }
    }

    @Override
    public byte[] serialize(final String topic, final Message data) {
        if (data == null) {
            return null;
        }

        try {
            return OBJECT_MAPPER.writeValueAsBytes(data);
        } catch (final Exception e) {
            throw new SerializationException("Error serializing JSON message", e);
        }
    }

    @Override
    public void close() {
    }

    @Override
    public Serializer<Message> serializer() {
        return this;
    }

    @Override
    public Deserializer<Message> deserializer() {
        return this;
    }
}