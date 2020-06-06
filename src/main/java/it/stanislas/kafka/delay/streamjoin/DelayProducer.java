package it.stanislas.kafka.delay.streamjoin;

import io.reactivex.rxjava3.core.Observable;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static it.stanislas.kafka.delay.streamjoin.ProducerFactory.buildProducer;

public class DelayProducer {
    private final KafkaProducer<String, String> kafkaProducer;
    private final String delayTopicName;

    final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyyMMddHHmmss").withZone( ZoneId.systemDefault());

    protected DelayProducer(KafkaProducer<String, String> kafkaProducer, String delayTopicName) {
        this.kafkaProducer = kafkaProducer;
        this.delayTopicName = delayTopicName;
    }

    public static DelayProducer build(final String bootstrapServers, final String delayTopicName) {
        return new DelayProducer(buildProducer(bootstrapServers), delayTopicName);
    }

    public void sendAMessageEachMinute() {
        Observable
                .interval(1, TimeUnit.MINUTES)
                .doOnNext(n -> {
//                .doOnNext(t -> {
//                    IntStream.range(0, 10)
//                            .forEach(n -> {
                                final Instant now = Instant.now();
                                final Instant fireAt = now.plus(1, ChronoUnit.MINUTES);
                                final String value = String.valueOf(n);
                                final Future<RecordMetadata> recordMetadataFeature = sendAt(value, fireAt);
//                            });
                })
                .subscribe();
    }

    public Future<RecordMetadata> sendAt(final String message, final Instant fireAt) {
        final Instant now = Instant.now();
        final String key = formatter.format(fireAt);
        final String value = message;
        final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(delayTopicName, key, value));
        System.out.println("send k:" + key + " v:" + value + " fireTime:" + formatter.format(now));
        return recordMetadataFeature;
    }

}
