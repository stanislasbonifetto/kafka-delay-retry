package it.stanislas.kafka.delay;

import io.reactivex.rxjava3.core.Observable;
import it.stanislas.kafka.delay.streamjoin.ClockProducer;
import it.stanislas.kafka.delay.streamjoin.DelayProducer;
import it.stanislas.kafka.delay.streamjoin.DelayStream;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.ClassRule;
import org.junit.Ignore;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class DelayWithStreamsTest {
    @ClassRule
    public final static KafkaContainer kafka = new KafkaContainer();

    //given
    final String bootstrapServers = kafka.getBootstrapServers();
//    final String bootstrapServers = "localhost:9092";

    // topic-delay
    final String delayTopicName = "topic-delay";

    // topic-clock
    final String clockTopicName = "topic-clock";

    // topic-fired
    final String firedTopicName = "topic-fired";

    final DelayStream delayStream = DelayStream.buildAndStart(bootstrapServers, delayTopicName, clockTopicName, firedTopicName);

    final ClockProducer clockProducer = ClockProducer.buildAndStart(bootstrapServers, clockTopicName);

    final DelayProducer delayProducer = DelayProducer.build(bootstrapServers, delayTopicName);

    final Consumer firedConsumer = buildKafkaConsumer(bootstrapServers, firedTopicName);

    @Test
    public void delay_a_message() {

        //when
        final Instant now = Instant.now();
        final Instant fireAt = now.plus(10, ChronoUnit.SECONDS);
        final String message = "my-message";
        delayProducer.sendAt(message, fireAt);

        //then
        await().atMost(1, MINUTES).until(() -> {
            final ConsumerRecords<String, String> records = firedConsumer.poll(Duration.ofSeconds(1));
            records.forEach(r -> System.out.println("fired k:" + r.key() + " v:" + r.value()));
            return assertRecord(records, message, fireAt);
        });

    }

    @Test
    @Ignore
    public void delay_messages() {

        //when
        delayProducer.sendAMessageEachMinute();

        //then
        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    final ConsumerRecords<String, String> records = firedConsumer.poll(Duration.ofSeconds(1));
                    records.forEach(r -> {
                        System.out.println("fired k:" + r.key() + " v:" + r.value());
                    });
                })
                .subscribe();


        try {
            Thread.sleep(10 * 60 * 1000);
        } catch (Exception e) {}

    }

    private static Boolean assertRecord(ConsumerRecords<String, String> records, final String expectedKey, final Instant expectedAt) {
        if(records.count() <= 0) return false;
        final ConsumerRecord record = records.iterator().next();

        final Instant recordTimestamp = Instant.ofEpochMilli(record.timestamp());

        final Duration diffFireTime = Duration.between(expectedAt, recordTimestamp);

        final long diffFireTimeInSeconds = diffFireTime.getSeconds();

//        System.out.println("diff :" + diffFireTime.getSeconds());

        final boolean keyIsEqual = record.key().equals(expectedKey);

        final boolean isFireOnTime = diffFireTimeInSeconds > -2 && diffFireTimeInSeconds < 2;

//        System.out.println("keyIsEqual :" + keyIsEqual + " isFireOnTime: "+ isFireOnTime);

        return keyIsEqual && isFireOnTime;
    }

    private Consumer<String, String> buildKafkaConsumer(final String bootstrapServers, final String topicName) {
        final Properties props = new Properties();
        final UUID uuid = UUID.randomUUID();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer-"+ uuid.toString());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }

}
