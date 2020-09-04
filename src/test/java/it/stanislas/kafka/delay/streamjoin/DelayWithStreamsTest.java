package it.stanislas.kafka.delay.streamjoin;

import io.reactivex.rxjava3.core.Observable;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.streams.StreamsConfig;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.awaitility.Awaitility.await;

@Testcontainers
public class DelayWithStreamsTest {

    @Container
    final static KafkaContainer KAFKA_CONTAINER = new KafkaContainer();

    static String bootstrapServers;
//    final static String bootstrapServers = "localhost:9092";

    // topic-delay
    final static String DELAY_TOPIC_NAME = "delay";

    // topic-clock
    final static String CLOCK_TOPIC_NAME = "clock";

    // topic-fired
    final static String FIRED_TOPIC_NAME = "fired";

    final ClockKafkaKeyGenerator clockKafkaKeyGenerator = new ClockKafkaKeyGenerator();

    @BeforeAll
    public static void setup() {
        //given
        createTopics();

    }

    private static void createTopics() {
        bootstrapServers = KAFKA_CONTAINER.getBootstrapServers();

        final Properties config = new Properties();
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        final AdminClient adminClient = AdminClient.create(config);
        final NewTopic delayTopic = new NewTopic(DELAY_TOPIC_NAME, 1, (short)1);
        final NewTopic clockTopic = new NewTopic(CLOCK_TOPIC_NAME, 1, (short)1);
        final NewTopic firedTopic = new NewTopic(FIRED_TOPIC_NAME, 1, (short)1);

        adminClient.createTopics(Arrays.asList(delayTopic, clockTopic, firedTopic));
        adminClient.close();
    }


    @Test
    public void delay_a_message() {

        //given
        final DelayStream delayStream = DelayStream.buildAndStart(bootstrapServers, DELAY_TOPIC_NAME, CLOCK_TOPIC_NAME, FIRED_TOPIC_NAME);

        final ClockProducer clockProducer = ClockProducer.buildAndStart(bootstrapServers, CLOCK_TOPIC_NAME, clockKafkaKeyGenerator);

        final DelayProducer delayProducer = DelayProducer.build(bootstrapServers, DELAY_TOPIC_NAME, clockKafkaKeyGenerator);

        final Consumer firedConsumer = buildKafkaConsumer(bootstrapServers, FIRED_TOPIC_NAME);

        //when
        // I fire a message with 10s delay
        final Instant now = Instant.now();
        final Instant fireAt = now.plus(10, ChronoUnit.SECONDS);
        final String messageKey = "my-key";
        final String messageValue = "my-value";

        // I set the message value with format "key:value"
        final String message = messageKey + ":" + messageValue;
        delayProducer.sendAt(message, fireAt);

        //then
        // I consume the fired topic to check if the message is fired and check the timestamp of the message
        await().atMost(1, MINUTES).until(() -> {
            final ConsumerRecords<String, String> records = firedConsumer.poll(Duration.ofSeconds(1));
            records.forEach(r -> System.out.println("fired k:" + r.key() + " v:" + r.value()));
            return assertMessageIsFiredAtTheRightTime(records, messageKey, messageValue, fireAt);
        });

    }

    private static Boolean assertMessageIsFiredAtTheRightTime(ConsumerRecords<String, String> records, final String expectedKey,final String expectedValue, final Instant expectedAt) {
        if(records.count() <= 0) return false;
        final ConsumerRecord record = records.iterator().next();

        final Instant recordTimestamp = Instant.ofEpochMilli(record.timestamp());

        final Duration diffFireTime = Duration.between(expectedAt, recordTimestamp);

        final long diffFireTimeInSeconds = diffFireTime.getSeconds();

        System.out.println("diff :" + diffFireTime.getSeconds());

        final boolean keyIsEqual = record.key().equals(expectedKey);
        final boolean valueIsEqual = record.value().equals(expectedValue);

        final boolean isFireOnTime = diffFireTimeInSeconds > -2 && diffFireTimeInSeconds < 2;

//        System.out.println("keyIsEqual :" + keyIsEqual + " valueIsEqual :" + valueIsEqual + " isFireOnTime: "+ isFireOnTime);

        return keyIsEqual && valueIsEqual && isFireOnTime;
    }

    @Test
    @Disabled //this test is to just run and print it doesn't any validation
    public void delay_messages() {
        //given
        final DelayStream delayStream = DelayStream.buildAndStart(bootstrapServers, DELAY_TOPIC_NAME, CLOCK_TOPIC_NAME, FIRED_TOPIC_NAME);

        final ClockProducer clockProducer = ClockProducer.buildAndStart(bootstrapServers, CLOCK_TOPIC_NAME, clockKafkaKeyGenerator);

        final DelayProducer delayProducer = DelayProducer.build(bootstrapServers, DELAY_TOPIC_NAME, clockKafkaKeyGenerator);

        final Consumer firedConsumer = buildKafkaConsumer(bootstrapServers, FIRED_TOPIC_NAME);

        //when
        sendAMessageDelayedOneMinutesEachThenSeconds(delayProducer);

        //then
        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    final ConsumerRecords<String, String> records = firedConsumer.poll(Duration.ofSeconds(1));
                    records.forEach(r -> System.out.println("fired k:" + r.key() + " v:" + r.value()));
                })
                .subscribe();


        try {
            Thread.sleep(10 * 60 * 1000);
        } catch (Exception e) {}

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

    public void sendAMessageDelayedOneMinutesEachThenSeconds(final DelayProducer delayProducer) {
        Observable
                .interval(10, TimeUnit.SECONDS)
                .doOnNext(n -> {
//                .doOnNext(t -> {
//                    IntStream.range(0, 10)
//                            .forEach(n -> {
                    final Instant now = Instant.now();
                    final Instant fireAt = now.plus(1, ChronoUnit.MINUTES);
                    final String value = String.valueOf(n);
                    final Future<RecordMetadata> recordMetadataFeature = delayProducer.sendAt(value, fireAt);
//                            });
                })
                .subscribe();
    }

}
