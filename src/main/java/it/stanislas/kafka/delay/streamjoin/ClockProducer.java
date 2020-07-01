package it.stanislas.kafka.delay.streamjoin;

import io.reactivex.rxjava3.core.Observable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static it.stanislas.kafka.delay.streamjoin.ProducerFactory.buildProducer;

public class ClockProducer {

    private final KafkaProducer<String, String> kafkaProducer;
    private final String clockTopicName;

    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone( ZoneId.systemDefault());

    protected ClockProducer(KafkaProducer<String, String> kafkaProducer, final String clockTopicName) {
        this.kafkaProducer = kafkaProducer;
        this.clockTopicName = clockTopicName;
    }

    public static ClockProducer buildAndStart(final String bootstrapServers, final String clockTopicName) {
        final ClockProducer producer = build(bootstrapServers, clockTopicName);
        producer.start();
        return producer;
    }

    public static ClockProducer build(final String bootstrapServers, final String clockTopicName) {
        return new ClockProducer(buildProducer(bootstrapServers), clockTopicName);
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
        final Instant now = Instant.now();
        final String key = formatter.format(now);
        final String value = key;
        final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(clockTopicName, key, value));
//        System.out.println("tick k:" + key + " v:" + value);
    }
}
