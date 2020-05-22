package it.stanislas.kafka.delay;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.reactivex.rxjava3.core.Observable;
import it.stanislas.kafka.delay.model.MessageA;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/***
 * produce a message A in topic-a
 */
public class ProducerA {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

    private final KafkaProducer<String ,String> kafkaProducer;
    private final String topic;

    private ProducerA(final KafkaProducer<String ,String> kafkaProducer,final String topic) {
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

    private static KafkaProducer<String ,String> createKafkaProducer(final KafkaConfig kafkaConfig) {
        final Properties producerConfig = new Properties();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.bootstrapSever());
        producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final KafkaProducer kafkaProducer = new KafkaProducer<String, String>(producerConfig);
        return kafkaProducer;
    }

    public void start() {
        Observable
                .interval(1, TimeUnit.SECONDS)
                .doOnNext(n -> {
                    final String key = n.toString();
                    final MessageA value = new MessageA(n.toString());
                    final String MessageAJson = OBJECT_MAPPER.writeValueAsString(value);
                    final Future<RecordMetadata> recordMetadataFeature =  kafkaProducer.send(new ProducerRecord(topic, key,MessageAJson));
//                    System.out.println("send k:" + key + " v:" + value);
                })
                .subscribe();
    }
}
