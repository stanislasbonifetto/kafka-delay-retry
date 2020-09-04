package it.stanislas.kafka.delay.streamjoin;

import io.reactivex.rxjava3.core.Observable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static it.stanislas.kafka.delay.streamjoin.ProducerFactory.buildProducer;

public class ClockProducer {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String clockTopicName;
    private final ClockKafkaKeyGenerator clockKafkaKeyGenerator;

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

        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    sendTick();
                })
                .subscribe();

        //simulate distributed clock producer
        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    sendTick();
                })
                .subscribe();

    }

    private void sendTick() {
        final String key = clockKafkaKeyGenerator.now();
        final String value = key;
        final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(clockTopicName, key, value));
//        System.out.println("tick k:" + key + " v:" + value);
    }
}
