package it.stanislas.kafka.delay.processor;

import it.stanislas.kafka.delay.processor.model.MessageA;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

public class MessageASerde implements Serde<MessageA> {
    @Override
    public Serializer<MessageA> serializer() {
        return null;
    }

    @Override
    public Deserializer<MessageA> deserializer() {
        return null;
    }
}
