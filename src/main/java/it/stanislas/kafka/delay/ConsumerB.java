package it.stanislas.kafka.delay;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

/***
 * consume messages from topic-b and print
 */
public class ConsumerB {

    final Consumer<String, String> consumer;

    public ConsumerB() {
        consumer = new KafkaConsumer<>(createProperties());
        consumer.subscribe(Collections.singletonList("topic-b"));
    }

    public void start(){
        Thread t = new Thread(() -> {
            while(true) {
                final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()){
                    System.out.println("Records empty");
                    continue;
                }
                records.forEach(r -> System.out.println("k:" + r.key() + " v:" + r.value()));
            }
        });
        t.start();
    }

    private Properties createProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        return props;
    }
}