package it.stanislas.kafka.delay.streamjoin;

import io.reactivex.rxjava3.core.Observable;
import io.reactivex.rxjava3.disposables.Disposable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;

import static it.stanislas.kafka.delay.streamjoin.ProducerFactory.buildProducer;

public class ClockProducer {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String clockTopicName;
    private final ClockKafkaKeyGenerator clockKafkaKeyGenerator;
    private final int numberOfInstances = 2;

    protected ClockProducer(final KafkaProducer<String, String> kafkaProducer,
                            final String clockTopicName,
                            final ClockKafkaKeyGenerator clockKafkaKeyGenerator
    ) {
        this.kafkaProducer = kafkaProducer;
        this.clockTopicName = clockTopicName;
        this.clockKafkaKeyGenerator = clockKafkaKeyGenerator;
    }

    public static ClockProducer buildAndStart(
            final String bootstrapServers,
            final String clockTopicName,
            final ClockKafkaKeyGenerator clockKafkaKeyGenerator
    ) {
        final ClockProducer producer = build(bootstrapServers, clockTopicName, clockKafkaKeyGenerator);
        producer.start();
        return producer;
    }

    public static ClockProducer build(
            final String bootstrapServers,
            final String clockTopicName,
            final ClockKafkaKeyGenerator clockKafkaKeyGenerator
    ) {
        return new ClockProducer(buildProducer(bootstrapServers), clockTopicName, clockKafkaKeyGenerator);
    }

    public void start() {
        IntStream.range(0, numberOfInstances)
                .forEach(i -> sendTickEachSecond());
    }

    private Disposable sendTickEachSecond() {
        return Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> sendTick())
                .subscribe();
    }

    private void sendTick() {
        final String key = clockKafkaKeyGenerator.now();
        final String value = key;
        final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(clockTopicName, key, value));
//        System.out.println("tick k:" + key + " v:" + value);
    }
}
