package it.stanislas.kafka.delay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/***
 * consume messages from topic-b and print
 */
public class ConsumerB {

    private final Consumer<String, String> kafkaConsumer;

    private ConsumerB(Consumer<String, String> kafkaConsumer) {
        this.kafkaConsumer = kafkaConsumer;
    }

    public static ConsumerB createAndStart(final KafkaConfig kafkaConfig) {
        final ConsumerB consumer = new ConsumerB(createKafkaConsumer(kafkaConfig));
        consumer.start();
        return consumer;
    }

    private static Consumer<String, String> createKafkaConsumer(final KafkaConfig kafkaConfig) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(kafkaConfig.destinationTopic()));
        return consumer;
    }

    public void start() {
        Thread t = new Thread(() -> {
            while(true) {
                final ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(1));
                System.out.println("pull new messages");
                records.forEach(r -> {
                    System.out.println("k:" + r.key() + " v:" + r.value());
                });
            }
        });
        t.start();
    }
}
