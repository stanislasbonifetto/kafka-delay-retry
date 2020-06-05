package it.stanislas.kafka.delay;

import io.reactivex.rxjava3.core.Observable;
import it.stanislas.kafka.delay.model.MessageA;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.junit.ClassRule;
import org.junit.Test;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

@Testcontainers
public class DelayWithStreamsTest {
    @ClassRule
    public final static KafkaContainer kafka = new KafkaContainer();

    @Test
    public void delay_with_streams() {
        //given
//        final String bootstrapServers = kafka.getBootstrapServers();
        final String bootstrapServers = "localhost:9092";

        // topic-delay
        final String delayTopicName = "topic-delay";

        // topic-tick
        final String tickTocTopicName = "topic-tick-toc";

        // topic-fired
        final String firedTopicName = "topic-fired";

        final Producer delayProducer = buildProducer(bootstrapServers);

        final Producer tickTocProducer = buildProducer(bootstrapServers);

        final Consumer firedConsumer = buildKafkaConsumer(bootstrapServers, firedTopicName);

        final KafkaStreams kafkaStreams = buildDelayStream(bootstrapServers, delayTopicName, tickTocTopicName, firedTopicName);


        final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone( ZoneId.systemDefault());


        kafkaStreams.start();

        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    final Instant now = Instant.now();
                    final String key = formatter.format(now);
                    final String value = key;
                    final Future<RecordMetadata> recordMetadataFeature = tickTocProducer.send(new ProducerRecord(tickTocTopicName, key, value));
//                    System.out.println("tick k:" + key + " v:" + value);
                })
                .subscribe();

        //simulate distributed ticktoc producer
        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    final Instant now = Instant.now();
                    final String key = formatter.format(now);
                    final String value = key;
                    final Future<RecordMetadata> recordMetadataFeature = tickTocProducer.send(new ProducerRecord(tickTocTopicName, key, value));
//                    System.out.println("tick k:" + key + " v:" + value);
                })
                .subscribe();


//        try {
//            Thread.sleep( 5 * 1000);
//        } catch (Exception e) {}

        //when
        Observable
                .interval(1, TimeUnit.MINUTES)
//                .doOnNext(n -> {
                .doOnNext(t -> {
                    IntStream.range(0, 10)
                            .forEach(n -> {
                                final Instant now = Instant.now();
                                final Instant current = now.plus(1, ChronoUnit.MINUTES);
                                final String key = formatter.format(current);
                                final String value = String.valueOf(n);
                                final Future<RecordMetadata> recordMetadataFeature = delayProducer.send(new ProducerRecord(delayTopicName, key, value));
                                System.out.println("send k:" + key + " v:" + value + " fireTime:" + formatter.format(now));
                            });
                })
                .subscribe();

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

        // send messages delayed

        //then

        // it fired at the right time

    }

    private KafkaProducer<String, String> buildProducer(final String bootstrapServers) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        final KafkaProducer kafkaProducer = new KafkaProducer<String, MessageA>(producerConfig);
        return kafkaProducer;
    }

    private Consumer<String, String> buildKafkaConsumer(final String bootstrapServers, final String topicName) {
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // Create the consumer using props.
        final Consumer<String, String> consumer =
                new KafkaConsumer<>(props);

        // Subscribe to the topic.
        consumer.subscribe(Collections.singletonList(topicName));
        return consumer;
    }

    private KafkaStreams buildDelayStream(final String bootstrapServers, final String delayTopicName, final String tickTocTopicName, final String firedTopicName) {

        final Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "processor-a-to-b-stream");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        StreamsBuilder builder = new StreamsBuilder();

        KStream<String, String> tickToc = builder.stream(tickTocTopicName, Consumed.with(Serdes.String(), Serdes.String()));
        KStream<String, String> delayed = builder.stream(delayTopicName, Consumed.with(Serdes.String(), Serdes.String()));

        KStream<String, String> groupByTickToc = tickToc.groupByKey().reduce((value1, value2) -> value1).toStream();

        delayed.join(groupByTickToc,
                (delayedValue, tickTocValue) ->  delayedValue + "/" + tickTocValue,
                JoinWindows.of(Duration.ofDays(1)),
                StreamJoined.with(Serdes.String(), Serdes.String(), Serdes.String())
        ).to(firedTopicName);

//        builder.table(firedTopicName, Materialized.as("table"));

        final Topology topology = builder.build();

        final KafkaStreams streams = new KafkaStreams(topology, config);
        return streams;
    }

}
