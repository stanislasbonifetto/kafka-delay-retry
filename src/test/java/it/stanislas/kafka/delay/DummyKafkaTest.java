package it.stanislas.kafka.delay;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

@Testcontainers
public class DummyKafkaTest {

    @ClassRule
    public final static KafkaContainer kafka = new KafkaContainer();

    @Test
    public void kafka_is_up() {

        final String topicName = "my-topic";

        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer kafkaProducer = new KafkaProducer<String, String>(producerConfig);

        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "my-consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer = new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topicName));
        consumer.poll(Duration.ofSeconds(1));

        final String key = "my-key";
        final String value = "my-value";

        kafkaProducer.send(new ProducerRecord(topicName, key,value));
        kafkaProducer.flush();

        //TODO: refactoring the assertion is basic and not well readable
        await().atMost(30, SECONDS).until(() -> {
            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
            return assertRecord(records, key, value);
        });

    }

    private static Boolean assertRecord(ConsumerRecords<String, String> records, final String expectedKey, final String expectedValue) {
        if(records.count() <= 0) return false;
        final ConsumerRecord record = records.iterator().next();
        return record.key().equals(expectedKey) && record.value().equals(expectedValue);
    }
}
