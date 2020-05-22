package it.stanislas.kafka.delay;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Observable;
import it.stanislas.kafka.delay.model.MessageA;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/***
 * produce a message A in topic-a
 */
public class ProducerA {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaProducer<String, MessageA> kafkaProducer;
    private final String topic;

    private ProducerA(final KafkaProducer<String, MessageA> kafkaProducer, final String topic) {
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    public static ProducerA createAndStart(final KafkaConfig kafkaConfig) {
        final ProducerA producer = create(kafkaConfig);
        producer.start();
        return producer;
    }

    public static ProducerA create(final KafkaConfig kafkaConfig) {
        return new ProducerA(createKafkaProducer(kafkaConfig), kafkaConfig.sourceTopic());
    }

    private static KafkaProducer<String, MessageA> createKafkaProducer(final KafkaConfig kafkaConfig) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JSONSerde.class.getName());

        final KafkaProducer kafkaProducer = new KafkaProducer<String, MessageA>(producerConfig);
        return kafkaProducer;
    }

    public void start() {

        Observable
                .interval(1, TimeUnit.SECONDS)
                .take(5)
                .doOnNext(n -> {
                    final String key = n.toString();
                    final Instant current = Instant.now().plus(5 * n, ChronoUnit.SECONDS);
                    final MessageA value = new MessageA(n.toString(), current.toEpochMilli());
                    final Future<RecordMetadata> recordMetadataFeature = kafkaProducer.send(new ProducerRecord(topic, key, value));
//                    System.out.println("send k:" + key + " v:" + value);
                })
                .subscribe();
    }
}
