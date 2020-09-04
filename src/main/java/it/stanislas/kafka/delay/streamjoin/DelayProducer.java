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
    private final ClockKafkaKeyGenerator clockKafkaKeyGenerator;

    protected DelayProducer(final KafkaProducer<String, String> kafkaProducer,
                            final String delayTopicName,
                            final ClockKafkaKeyGenerator clockKafkaKeyGenerator
    ) {
        this.kafkaProducer = kafkaProducer;
        this.delayTopicName = delayTopicName;
        this.clockKafkaKeyGenerator = clockKafkaKeyGenerator;
    }

    public static DelayProducer build(
            final String bootstrapServers,
            final String delayTopicName,
            final ClockKafkaKeyGenerator clockKafkaKeyGenerator
    ) {
        return new DelayProducer(buildProducer(bootstrapServers), delayTopicName, clockKafkaKeyGenerator);
    }

    public void sendAMessageDelayedOneMinutesEachThenSeconds() {
        Observable
                .interval(10, TimeUnit.SECONDS)
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
        final String fireTime = clockKafkaKeyGenerator.now();
        final String key = clockKafkaKeyGenerator.at(fireAt);
        final String value = message;
        final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(delayTopicName, key, value));
        System.out.println("send k:" + key + " v:" + value + " fireTime:" + fireTime);
        return recordMetadataFeature;
    }

}
