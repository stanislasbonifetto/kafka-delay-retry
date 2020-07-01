package it.stanislas.kafka.delay.streamjoin;

import it.stanislas.kafka.delay.model.MessageA;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class ProducerFactory {
    protected static KafkaProducer<String, String> buildProducer(final String bootstrapServers) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        final KafkaProducer kafkaProducer = new KafkaProducer<String, MessageA>(producerConfig);
        return kafkaProducer;
    }
}
