package it.stanislas.kafka.delay;

import com.fasterxml.jackson.databind.ObjectMapper;
import it.stanislas.kafka.delay.model.MessageB;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/***
 * consume messages from topic-b and print
 */
public class ConsumerB {
    private final static String GROUP_ID = "consumer-b";

    private final Consumer<String, MessageB> kafkaConsumer;

    private ConsumerB(Consumer<String, MessageB> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public static ConsumerB createAndStart(final KafkaConfig kafkaConfig) {
        final ConsumerB consumer = new ConsumerB(createKafkaConsumer(kafkaConfig));
        consumer.start();
        return consumer;
    }

    private static Consumer<String, MessageB> createKafkaConsumer(final KafkaConfig kafkaConfig) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, GROUP_ID);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JSONSerde.class);

        // Create the consumer using props.
        final Consumer<String, MessageB> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(kafkaConfig.destinationTopic()));
        return consumer;
    }

    public void start() {
        Thread t = new Thread(() -> {
            while(true) {
                final ConsumerRecords<String, MessageB> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                records.forEach(r -> {
                    System.out.println("k:" + r.key() + " v:" + r.value());
                });
            }
        });
        t.start();
    }
}
